import time

from bson.objectid import ObjectId

import pymongo

from server.resources.models.brand import Brand
from server.resources.models.user import User
from server.services.aws_service import AWSService
from server.utilities.auth_utility import AuthUtility

from server.core.constants import Constants


log = AWSService().log_service


class UserService:
    """Manages CLI commands to modify user accounts."""
    
    def __init__(self, ssm_service, twilio_service):
        """Initializes instance.

        Args:
            ssm_service: Instance of AWSService.SSMService
            twilio_service: Instance of TwilioService
        """
        self._auth_utility = AuthUtility()
        self._aws_service = None
        self._ssm_service = ssm_service
        self._twilio_service = twilio_service
        
    def add(self, email, name, scopes, client):
        """Adds user to `users` collection.

        Args:
            email: User's email address
            name: User's full name
            scopes: List of permissions assigned to user
            client: DocumentDB client provided from AWSService.DocumentDBService

        Returns:
            None

        Raises:
            pymongo.errors.OperationFailure: An error occurred when creating a user document
        """
        password = self._auth_utility.encrypt_password(
            self._auth_utility.password(),
        )
        
        try:
            User.create(
                {
                    'email': email,
                    'full_name': name,
                    'password': password,
                    'scopes': scopes,
                },
                client,
            )
        except pymongo.errors.OperationFailure as e:
            log.exception(e)
            

    def add_brands(self, user_id, brand_ids, client):
        """Adds brands to a user and adds the user to the brand's users.

        Args:
            user_id: User identifier
            brand_ids: List of brand identifiers
            client: DocumentDB client provided from AWSService.DocumentDBService
        """
        user = User.find_by_id(
            ObjectId(user_id),
            client,
        )

        if user:
            log.info(
                f'Adding brands {brand_ids} to {user_id}...',
            )
            for brand_id in brand_ids:
                object_brand_id = ObjectId(brand_id)
                if object_brand_id not in user.brands:
                    log.info(
                        f'Adding brand {brand_id} to user...',
                    )

                    User.add_brand(
                        user._id,
                        object_brand_id,
                        client,
                    )
                    log.info(
                        f'Added brand {brand_id} to user',
                    )

                brand = Brand.find_by_id(
                    object_brand_id,
                    client,
                )

                if user._id not in brand.users:
                    log.info(
                        f'Adding user {user_id} to brand {brand_id}...',
                    )
                    Brand.add_user(
                        object_brand_id,
                        user._id,
                        client,
                    )
                    log.info(
                        f'Added user {user_id} to brand {brand_id}',
                    )     

            log.info(
                f'Added brands {brand_ids} to {user_id}',
            )
        else:
            log.info(
                f'User {user_id} does not exist',
            )  

    def invite(self, email, brands, client):
        """Invites an existing user to Visibly.

        This method also adds brands to the user.

        Args:
            email: User's email address
            brands: List of brand names
            client: DocumentDB client provided from AWSService.DocumentDBService

        Returns:
            Partial URL that Twilio uses to in link emailed to user
        """
        user = User.find_by_email(email, client)
        
        for brand_name in brands:
            brand = Brand.find_by_name(
                brand_name,
                client,
            )

            log.info(
                f'Adding {user._id} to {brand.name}',
            )
            
            if brand and brand._id not in user.brands:
                brand = Brand.add_user(brand._id, user._id, client)
                user = User.add_brand(user._id, brand._id, client)
                
                log.info(
                    f'Added {user._id} to {brand_name}',
                )
            elif brand:
                if brand._id in user.brands:
                    log.error(
                        f'User already has access to {brand_name}',
                    )
                else:
                    log.error(
                        f'Brand {brand_name} does not exist',
                    )
                    return
        
        log.info(
            f'Inviting user {email} to {brands}',
        )

        invitation = {
            Constants.DATA: self._auth_utility.encrypt_parameters(
                {
                    Constants.PASS: user.password,
                },
                self.ssm_service.encryption_key,
            ),
            Constants.EMAIL: user.email,
            Constants.TS: int(time.time()),
        }

        hashed_invitation = self._auth_utility.encrypt_parameters(
            invitation,
            self.ssm_service.encryption_key,
        )

        return self._twilio_service.email_confirmation(
            user.email,
            hashed_invitation,
        )

    def remove_brands(self, user_id, brand_ids, client):
        """Removes brands from a user and a user from brands.

        Args:
            user_id: User identifier
            brand_ids: List of brand identifiers
            client: DocumentDB client provided from AWSService.DocumentDBService

        Returns:
            None
        """
        user = User.find_by_id(
            ObjectId(user_id),
            client,
        )

        brand_ids = [ObjectId(brand_id) for brand_id in brand_ids]

        if user:
            for brand_id in brand_ids:
                object_brand_id = ObjectId(brand_id)
                if object_brand_id in user.brands:
                    log.info(
                        f'Removing brand {brand_id} from {user_id}...',
                    )        
                    User.remove_brands(
                        user._id,
                        [object_brand_id],
                        client,
                    )
                    log.info(
                        f'Removed brands {brand_id} from {user_id}',
                    )

                    log.info(
                        f'Removing user {user_id} from brand {brand_id}...',
                    )
                    Brand.remove_user(
                        object_brand_id,
                        user._id,
                        client,
                    )
                    log.info(
                        f'Removed user {user_id} from brand {brand_id}',
                    ) 
        else:
            log.info(
                f'User {user_id} does not exist',
            )
    
    @property
    def ssm_service(self):
        """Instance of AWSService.SSMService."""
        return self._ssm_service

    @property
    def twilio_service(self):
        """Instance of TwilioService."""
        return self._twilio_service

    