import time

from twilio.base.exceptions import TwilioRestException
from twilio.rest import Client

from server.core.constants import Constants
from server.services.aws_service import AWSService
from server.utilities.auth_utility import AuthUtility


log = AWSService().log_service

class TwilioService:
    """Wrapper around Twilio's Verify API."""

    def __init__(self):
        self._account_sid = None
        self._auth_token = None
        self._client = None

        self._auth_utility = AuthUtility()
        self._service = None
        self._ssm_service = AWSService().ssm_service

    def authenticate_two_factor(self, email):
        """Emails TFA confirmation link to user's email.

        Args:
            email: User's email address

        Returns:
            Unique identifier shared with Twilio when requesting TFA via email
        """
        sid = self._sid(email)
        url = f'{self.ssm_service.uri}/#/auth/confirm?sid={sid}&code='

        self.client.verify.services(
            self.service,
        ).verifications.create(
            to=email,
            channel=Constants.EMAIL,
            channel_configuration={
                Constants.TEMPLATE_ID: self.ssm_service.twilio_confirmation_template,
                Constants.SUBSTITUTIONS: {
                    Constants.EMAIL: email,
                    Constants.URL: url,
                }
            }
        )
        
        return sid

    def authenticate_email(self, code, email):
        """Verifies code from user's TFA.

        Args:
            code: Six-digit code shared via user's email
            email: User's email address

        Returns:
            True if user is authenticated
            False otherwise

        Raises:
            TwilioRestException: An error occurred verifying user's code
            and/or email address with Twilio's Verify API
        """
        response = None
        try:
            response = self.client.verify.services(
                self.service,
            ).verification_checks.create(
                code=code,
                to=email,
            )
        except TwilioRestException as e:
            log.exception(e)

        is_valid_response = response.valid
        is_approved_response = response.status == Constants.APPROVED

        return is_valid_response and is_approved_response
    
    def email_confirmation(self, email, hash):
        """Emails invitation to user.

        Args:
            email: User's email address
            hash: Hash of user information

        Returns:
            Partial URL that Twilio uses to in link emailed to user
        """
        url = f'{self.ssm_service.uri}/#/auth/invite?hash={hash}&code='
        
        self.client.verify.services(
            self.service,
        ).verifications.create(
            to=email,
            channel=Constants.EMAIL,
            channel_configuration={
                Constants.TEMPLATE_ID: self.ssm_service.twilio_invitation_template,
                Constants.SUBSTITUTIONS: {
                    Constants.EMAIL: email,
                    Constants.URL: url,
                }
            }
        )
        return url

    def wants_reset_password(self, email):
        """Emails user a link to reset their password.

        Args:
            email: User's email address

        Returns:
            Unique identifier shared with Twilio when requesting TFA via email
        """
        sid = self._sid(email)
        url = f'{self.ssm_service.uri}/#/auth/reset?sid={sid}&code='

        self.client.verify.services(
            self.service,
        ).verifications.create(
            to=email,
            channel=Constants.EMAIL,
            channel_configuration={
                Constants.TEMPLATE_ID: self.ssm_service.twilio_reset_password_template,
                Constants.SUBSTITUTIONS: {
                    Constants.EMAIL: email,
                    Constants.URL: url,
                }
            }
        )

        return sid
    
    @property
    def account_sid(self):
        """Visibly's account statement ID (sid) at Twilio."""
        if self._account_sid is None:
            self._account_sid = self.ssm_service.twilio_account_sid

        return self._account_sid

    @property
    def auth_token(self):
        """Twilio authorization token."""
        if self._auth_token is None:
            self._auth_token = self.ssm_service.twilio_auth_token

        return self._auth_token

    @property
    def client(self):
        """Twilio client."""
        if self._client is None:
            self._client = Client(
                self.account_sid,
                self.auth_token,
                self.service,
            )

        return self._client

    @property
    def service(self):
        """Twilio service configuration parameter."""
        if self._service is None:
            self._service = self.ssm_service.twilio_email_config_service
        
        return self._service

    @property
    def ses_service(self):
        # TODO(declan.ryan@getvisibly.com) Decide whether to use SES
        if self._ses_service is None:
            self._ses_service = AWSService().ses_service

        return self._ses_service
    
    @property
    def ssm_service(self):
        """Instance of AWSService.SSMService."""
        if self._ssm_service is None:
            self._ssm_service = AWSService().ssm_service

        return self._ssm_service

    def _sid(self, email):
        payload = {
            Constants.EMAIL: email,
            Constants.TS: int(time.time()),
        }
        return self._auth_utility.encrypt_parameters(
            payload,
            self.ssm_service.encryption_key,
        )