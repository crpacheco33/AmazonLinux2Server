from datetime import (
    timedelta,
)

import time
import uuid

from fastapi import HTTPException
from fastapi.params import Depends
from fastapi.security import (
    HTTPAuthorizationCredentials,
)
from jose import (
    JWTError,
    jwt,
)
from starlette.status import (
    HTTP_400_BAD_REQUEST,
)

import pymongo

from server.core.constants import Constants
from server.overrides.dict_override import Keypath
from server.resources.models.brand import Brand
from server.resources.models.user import User
from server.resources.schema.token import (
    AccessToken,
    RefreshToken,
)
from server.resources.types.data_types import (
    EmailType,
    UserStatusType,
)
from server.services.aws_service import AWSService
from server.utilities.auth_utility import AuthUtility


log = AWSService().log_service


class AuthService:
    """Manages user authentication.

    `AuthService` manages user signup using invitations, user signin, and
    two-factor authentication using Twilio Verify.
    """

    def __init__(self, ssm_service, twilio_service):
        """Initializes `AuthService` instance.
        
        Args:
            ssm_service: Instance of AWSService.SSMService
            twilio_service: Instance of TwilioService

        Returns:
            None
        """
        self._access_token_expiration_time = None
        self._application_secret_key = None
        self._confirm_account_email_template = None
        self._domain = None
        self._encryption_key = None
        self._invitation_email_template = None
        self._jwt_algorithm = None
        self._reset_password_email_template = None

        self._auth_utility = AuthUtility()
        self._aws_service = None
        self._ssm_service = ssm_service
        self._twilio_service = twilio_service

    def authenticate(self, authentication, client):
        """Authenticates user after confirming identity using TFA.

        If user is signing in for the first time, this method updates the user's
        status to `active`. If user is authenticated, this method creates the user's
        access token.

        Args:
            authentication: Encrypted hash passed by server on sign in and returned from Twilio
            client: DocumentDB client provided from AWSService.DocumentDBService
        
        Returns:
            If successful, a tuple of access token, expiration timestamp, and refresh token
            If unsuccessful, a tuple of False, None, and None
        """
        payload = self._auth_utility.decrypt_parameters(
            authentication.sid,
            self.ssm_service.encryption_key,
        )
        email = payload.get(Constants.EMAIL)

        is_authenticated = self._twilio_service.authenticate_email(
            authentication.code,
            email,
        )

        if is_authenticated:
            with client.start_session() as session:
                collection = client.visibly.users
                collection.find_one_and_update(
                    { 'email': email },
                    { '$set': { 'status': UserStatusType.ACTIVE } },
                )
            
        else:
            return (False, None, None,)
        
        user = User.find_by_email(
            email,
            client,
        )

        if user is None:
            return (False, None, None,)

        brands = Brand.with_user(
            user._id,
            client,
        )
        brand_id = str(brands[0]._id)
        
        access_token_details = AccessToken(
            id=str(user._id),
            brand=brand_id,
            email=user.email,
            full_name=user.full_name,
            scopes=[scope for scope in user.scopes],
        )

        access_token, expires_in = self._auth_utility.encode_token(
            access_token_details,
            timedelta(
                minutes=int(self.ssm_service.access_token_expire_minutes),
            ),
            self.ssm_service.application_secret,
        )

        users = client.visibly.users
        with client.start_session() as session:
            with session.start_transaction():
                users.find_one_and_update(
                    { 'email': email },
                    { '$set': { 'refresh_token': str(uuid.uuid4()) } },
                    session=session,
                )

        user = User.find_by_email(
            email,
            client,
        )

        refresh_token_details = RefreshToken(
            brand_id=brand_id,
            email=user.email,
            version=str(user.refresh_token),
        )
        refresh_token, _ = self._auth_utility.encode_token(
            refresh_token_details,
            timedelta(days=1),
            self.ssm_service.application_secret,
        )

        return (access_token, expires_in, refresh_token,)

    def authenticate_for_brand(self, brand_id, user, client):
        """Authenticates user when user switches brand from application.

        Args:
            brand_id: Unique identifier for brand
            user: User document from DocumentDB
            client: DocumentDB client provided from AWSService.DocumentDBService

        Returns:
            If successful, a tuple of access token, expiration timestamp, and refresh token
            If unsuccessful, a tuple of False, None, and None
        """
        brands = Brand.with_user(user._id, client)
        brand_ids = [str(brand._id) for brand in brands]

        if brand_id not in brand_ids:
            return (False, None , None,)

        access_token_details = AccessToken(
            id=str(user._id),
            brand=str(brand_id),
            email=user.email,
            full_name=user.full_name,
            scopes=[scope for scope in user.scopes],
        )

        access_token, expires_in = self._auth_utility.encode_token(
            access_token_details,
            timedelta(
                minutes=int(self.ssm_service.access_token_expire_minutes),
            ),
            self.ssm_service.application_secret,
        )

        with client.start_session() as session:
            collection = client.visibly.users
            collection.find_one_and_update(
                { 'email': user.email },
                { '$set': { 'refresh_token': str(uuid.uuid4()) } },
            )

        user = User.find_by_email(
            user.email,
            client,
        )
        
        refresh_token_details = RefreshToken(
            brand_id=str(brand_id),
            email=user.email,
            version=str(user.refresh_token),
        )
        refresh_token, _ = self._auth_utility.encode_token(
            refresh_token_details,
            timedelta(days=1),
            self.ssm_service.application_secret,
        )

        return (access_token, expires_in, refresh_token,)
    
    def invite(self, email, name, scopes, brands, client):
        """Invites new user to Visibly.

        This method can be called multiple times for the same user, however,
        it is not idempotent. It will not update a user's name or scopes, but
        it will add brands to the user's list of brands.

        Args:
            email: Email address of user
            name: Full name of user
            scopes: List of permissions assigned to the user
            brands: List of brands user can access
            client: DocumentDB client provided from AWSService.DocumentDBService

        Returns:
            Partial URL that Twilio uses to in link emailed to user

        Raises:
            ValueError: An error occurred when the user is no longer active
        """
        password = self._auth_utility.encrypt_password(
            self._auth_utility.password(),
        )
        
        try:
            user = User.find_by_email(
                email,
                client,
            )

            if user is None:
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
            raise ValueError(
                Constants.USER_INACTIVE,
            )

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
            f'Inviting user {name} to {brands}',
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

    def is_valid_token(self, token):
        """Validates access token.

        Args:
            token: Access token stored as a cookie on `web`

        Returns:
            A tuple of brand identifier, user email, and refresh token version

        Raises:
            JWTError: An error occurred decoding the access token
            Exception: An indeterminate error occurred when decoding the access token
        """
        try:
            payload = jwt.decode(
                token,
                self.ssm_service.application_secret,
                algorithms=[Constants.JWT_ALGORITHM],
            )
            brand_id = payload.get(Constants.BRAND_ID)
            email = payload.get(Constants.EMAIL)
            # TODO(declan.ryan@getvisibly.com) Rename `version` to something meaningful
            version = payload.get(Constants.VERSION)

            return brand_id, email, version
        except JWTError as e:
            log.exception(e)
            raise
        except Exception as e:
            log.exception(e)
            raise

    def refresh_token(self, refresh_token, client):
        """Refreshes access token.

        Args:
            refresh_token: Refresh token stored as HTTP only cookie from `web`
            client: DocumentDB client provided from AWSService.DocumentDBService

        Returns:
            A tuple of access token, expiration timestamp, and refresh token

        Raises:
            ValueError: An error occured when the user is not active, the
                refresh token expired, or the refresh token does not contain
                an email address
        """
        brand_id, email, version = self.is_valid_token(refresh_token)

        user = User.find_by_email(
            email,
            client,
        )

        if not user:
            raise ValueError(
                Constants.USER_INACTIVE,
            )

        # TODO(declan.ryan@getvisibly.com) Rename `version` to something meaningful
        if version != user.refresh_token:
            raise ValueError(
                Constants.REFRESH_TOKEN_EXPIRED,
            )

        if user:
            access_token_details = AccessToken(
                id=str(user._id),
                brand=brand_id,
                email=user.email,
                full_name=user.full_name,
                scopes=[scope for scope in user.scopes],
            )

            access_token, expires_in = self._auth_utility.encode_token(
                access_token_details,
                timedelta(
                    minutes=int(self.ssm_service.access_token_expire_minutes),
                ),
                self.ssm_service.application_secret,
            )
            
            user = User.find_by_email(
                email,
                client,
            )

            # TODO(declan.ryan@getvisibly.com) Rename `version` to something meaningful
            refresh_token_details = RefreshToken(
                brand_id=brand_id,
                email=user.email,
                version=user.refresh_token,
            )
            refresh_token, _ = self._auth_utility.encode_token(
                refresh_token_details,
                timedelta(days=1),
                self.ssm_service.application_secret,
            )

            return (access_token, expires_in, refresh_token,)
        else:
            raise ValueError(
                Constants.REFRESH_TOKEN_MISSING_EMAIL,
            )

    def register(self, invitation, client):
        """Registers a user after accepting an invitation.

        Args:
            invitation: Encrypted hash passed by server on sign in and returned from Twilio
            client: DocumentDB client provided from AWSService.DocumentDBService

        Returns:
            True if the user was registered successfully
            False if the user was not registered

        Raises:
            Exception: An error occurred decrypting the invitation
        """
        log.info(
            f'Decrypting invitation...',
        )
        try:
            decrypted_parameters = self._auth_utility.decrypt_parameters(
                invitation.hash,
                self.ssm_service.encryption_key,
            )
        except Exception as e:
            log.exception(e)
            raise

        log.info(
            f'Decrypted invitation',
        )

        log.info(
            f'Registering using invitation...'
        )
        invitation_password = self._invitation_password(
            decrypted_parameters['data'],
        )

        invited_user = self._invited_user(
            invitation_password,
            client,
        )
        
        if invited_user and invited_user.status == UserStatusType.PENDING:
            password = self._auth_utility.encrypt_password(
                invitation.password,
            )
            
            with client.start_session() as session:
                collection = client.visibly.users
                collection.update_one(
                    { 'email': invited_user.email },
                    { '$set': {
                        'password': password,
                        'status': UserStatusType.ACTIVE
                        }
                    },
                )
            
            log.info(
                f'Registered using invitation'
            )
            return True

        log.warning(
            f'Failed to register user using invitation {invitation}'
        )
        return False
    
    def sign_in(self, credentials, client):
        """Signs user in.

        Args:
            credentials: Pytantic schema of user's email and password
            client: DocumentDB client provided from AWSService.DocumentDBService

        Returns:
            Unique identifier shared with Twilio when requesting TFA via email

        Raises:
            ValueError: An error occurred when the credentials are invalid, the email
                or password is incorrect, or the user has not been registered
        """
        log.info(
            f'User {credentials.email} attempting to sign in',
        )
        user = User.find_by_email(
            credentials.email,
            client,
        )

        if not user:
            log.error(
                f'User {credentials.email} provided invalid credentials',
            )
            raise ValueError(
                Constants.INVALID_CREDENTIALS,
            )

        log.info(
            f'User {credentials.email} found',
        )
        
        log.info(
            f'Authenticating {credentials.email}',
        )
        is_authenticated_user = self._auth_utility.authenticate_password(
            credentials.password,
            user.password,
        )

        if not is_authenticated_user:
            log.error(
                f'Authenticating {credentials.email} failed',
            )
            raise ValueError(
                Constants.INVALID_EMAIL_OR_PASSWORD,
            )
        
        log.info(
            f'Authenticated {credentials.email}',
        )
        if user.status == UserStatusType.PENDING:
            log.info(
                f'{credentials.email} is pending confirmation of account',
            )
            raise ValueError(
                Constants.ACCOUNT_PENDING_CONFIRMATION,
            )

        sid = self._twilio_service.authenticate_two_factor(
            user.email,
        )    

        log.info(
            f'User {credentials.email} waiting for TFA',
        )

        return sid

    def will_resend_email(self, data, client):
        """Resends email to reset password.

        Args:
            data: Pydantic schema of user's email
            client: DocumentDB client provided from AWSService.DocumentDBService

        Returns:
            None
        """
        user = User.find_by_email(data.email, client)
        email_type = data.email_type

        log.info(
            f'Sending {email_type} request to {user.email} again...'
        )

        if user:
            is_active_user = user.status == UserStatusType.ACTIVE
            is_pending_user = user.status == UserStatusType.PENDING
            is_confirm_request = email_type == EmailType.CONFIRM
            is_reset_request = email_type == EmailType.RESET

            if (
                (is_active_user or is_pending_user) and is_confirm_request
            ):
                self.twilio_service.authenticate_two_factor(
                    user.email,
                )
            if (
                is_active_user and is_reset_request
            ):
                self._twilio_service.wants_reset_password(user.email)
            
            log.info(
                f'Sent {email_type} request to {user.email}',
            )
    
    def wants_reset_password(self, data, client):
        """Sends reset password link to user.

        Args:
            data: Pydantic schema of user's email
            client: DocumentDB client provided from AWSService.DocumentDBService

        Returns:
            Unique identifier shared with Twilio when requesting TFA via email

        Raises:
            HTTPException: An error occurred when the user does not exist
        """
        log.info(
            f'{data.email} wants to reset their password...'
        )
        if User.find_by_email(data.email, client):
            sid = self.twilio_service.wants_reset_password(data.email)

            log.info(
                f'Waiting for {data.email} to reset password...'
            )
            return sid
        
        log.info(
            f'User {data.email} does not exist',
        )

        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=f'User {data.email} does not exist',
        )

    def will_reset_password(self, data, client):
        """Resets user's password.

        Args:
            data: Pydantic schema of user's credentials
            client: DocumentDB client provided from AWSService.DocumentDBService

        Returns:
            True if password was reset successfully
            False otherwise
        """
        payload = self._auth_utility.decrypt_parameters(
            data.sid,
            self.ssm_service.encryption_key,
        )
        email = payload.get(Constants.EMAIL)

        log.info(
            f'Will reset password for {email}...',
        )

        is_authenticated = self._twilio_service.authenticate_email(
            data.code,
            email,
        )

        if is_authenticated:
            password = self._auth_utility.encrypt_password(
                data.password,
            )

            with client.start_session(causal_consistency=True) as session:
                collection = client.visibly.users
                collection.update_one(
                    { 'email': email },
                    { '$set': { 'password': password } },
                )

            log.info(
                f'Did reset password for {email}'
            )
            return True

        log.error(
            f'Unable to reset password for {email}'
        )
        return False
    
    @property
    def access_token_expiration_time(self):
        """TTL of an access token."""
        if self._access_token_expiration_time is None:
            self._access_token_expiration_time = Constants.ACCESS_TOKEN_EXPIRATION_TIME

        return self._access_token_expiration_time

    @property
    def application_secret_key(self):
        """Application's secret key."""
        if self._application_secret_key is None:
            self._application_secret_key = self.ssm_service.application_secret

        return self._application_secret_key

    @property
    def aws_service(self):
        """Instance of AWSService."""
        if self._aws_service is None:
            self._aws_service = AWSService()

        return self._aws_service

    @property
    def confirm_account_email_template(self):
        if self._confirm_account_email_template is None:
            self._confirm_account_email_template = self.ssm_service.confirm_account_email_template

        return self._confirm_account_email_template

    @property
    def domain(self):
        # TODO(declan.ryan@getvisibly.com) Deprecate if not used
        if self._domain is None:
            self._domain = self.ssm_service.domain

        return self._domain

    @property
    def encryption_key(self):
        """Encryption key used to encrypted hashes and passwords."""
        if self._encryption_key is None:
            self._encryption_key = self.ssm_service.encryption_key

        return self._encryption_key

    @property
    def invitation_email_template(self):
        """Identifier of Sendgrid email template for invitations."""
        if self._invitation_email_template is None:
            self._invitation_email_template = self.ssm_service.invitation_email_template

        return self._invitation_email_template

    @property
    def jwt_algorithm(self):
        """JWT algorithm used by `server`."""
        if self._jwt_algorithm is None:
            self._jwt_algorithm = self.ssm_service.jwt_algorithm

        return self._jwt_algorithm

    @property
    def reset_password_email_template(self):
        """Identifier of Sendgrid email template for resetting passwords."""
        if self._reset_password_email_template is None:
            self._reset_password_email_template = self.ssm_service.reset_password_email_template

        return self._reset_password_email_template

    @property
    def ssm_service(self):
        """Instance of AWSService.SSMService"""
        return self._ssm_service

    @property
    def twilio_service(self):
        """Instance of TwilioService"""
        return self._twilio_service

    def _invitation_password(self, invitation):
        invitation_dict = self._auth_utility.decrypt_parameters(
            invitation,
            self.ssm_service.encryption_key,
        )
        return invitation_dict.get(
            Constants.PASS,
        )

    def _invited_user(self, password, client):
        return User.find_by_password(
            password,
            client,
        )