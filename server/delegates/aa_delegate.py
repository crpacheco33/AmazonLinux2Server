"""Provides configuration data to amazon-api client.

The delegate provides a way to configure `amazon-api` with parameters
specific to `server`.
"""


class AADelegate:
    """Provides configuration data to amazon-api client.

    The delegate provides a way to configure amazon-api with parameters
    specific to `server`.
    """

    def __init__(self, api, region, version, ssm_service, shared_ssm_service=None):
        """Defines initial state of delegate.

        Args:
            api: Amazon Advertising API. One of 'sb', 'sd', 'sp', or 'dsp'
            region: Advertiser region. One of 'na', 'eu', or 'fe'
            version: API version. Specific to `api`
            ssm_service: Instance of AWSService.SSMService that provides parameters for `server`'s instance (cloud)
            shared_ssm_service: Instance of AWSService.SSMService that provides parameters for assumed IAM role that can
                access advertiser's access token
        """
        self._ssm_service = ssm_service
        self._shared_ssm_service = shared_ssm_service

        self._access_token = None
        self._api = api
        self._client_id = None
        self._client_secret = None
        self._region = region
        self._version = version

    @property
    def access_token(self):
        """Advertiser's API access token"""
        return self._shared_ssm_service.amazon_aa_access_token

    @property
    def api(self):
        """Amazon Advertising API abbreviation"""
        return self._api

    @property
    def client_id(self):
        """Advertiser's client identifier"""
        return self._ssm_service.amazon_aa_client_id
    
    @property
    def region(self):
        """Advertiser's region"""
        return self._region

    @property
    def version(self):
        """Amazon Advertising API version"""
        return self._version