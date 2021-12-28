"""Class wrappers around AWSServices and boto3 methods.

Each class (contained within AWSService) closely matches the boto3 API,
adding lightweight syntax to fit with `server`'s use case.
"""


from datetime import (
    datetime,
    timezone
)
from io import BytesIO
from urllib import parse

import logging
import sys
import time

import boto3
import pymongo
import watchtower

from botocore.credentials import RefreshableCredentials
from botocore.session import get_session
from elasticsearch import (
    Elasticsearch,
    RequestsHttpConnection,
)
from elasticsearch.helpers import bulk
from requests_aws4auth import AWS4Auth

from server.core.constants import Constants
from server.core import settings


class UTCFormatter(logging.Formatter):
    converter = time.gmtime


class AWSService:

    class DocumentDBService:

        def __init__(self, ssm_service, uri=None):
            self._client = None
            self._docdb = None
            self._endpoint = None
            self._password = None
            self._port = None
            self._uri = None
            self._user = None

            self._ssm_service = ssm_service

        @property
        def client(self):
            if self._client is None:
                self._client = pymongo.MongoClient(
                    self.uri,
                    w=1,
                    journal=True,
                    readPreference='primary',
                )

            return self._client
        
        @property
        def docdb(self):
            return self.client

        @property
        def endpoint(self):
            if self._endpoint is None:
                self._endpoint = self._ssm_service.documentdb_endpoint

            return self._endpoint

        @property
        def password(self):
            if self._password is None:
                self._password = self._ssm_service.documentdb_password

            return self._password

        @property
        def port(self):
            if self._port is None:
                self._port = self._ssm_service.documentdb_port

            return self._port

        @property
        def uri(self):
            if self._uri is None:
                import os

                database_uri = os.getenv('TEST_DATABASE_URI')
                if database_uri:
                    self._uri = database_uri
                else:
                    password = parse.quote_plus(self.password)
                    self._uri = f'mongodb://{self.user}:{password}@{self.endpoint}/?tls=true&tlsCAFile=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false',
            
            return self._uri

        @property
        def user(self):
            if self._user is None:
                self._user = self._ssm_service.documentdb_user

            return self._user

    class ECSService:

        def __init__(self, region):
            self._ecs = None
            self._region = region

        def tag_value_for_key(self, key):
            tags, value = self._tags(), None
            for tag in tags:
                if tag.get(Constants.KEY) == key:
                    value = tag.get(Constants.VALUE)

            return value

        @property
        def ecs(self):
            if self._ecs is None:
                self._ecs = boto3.client(
                    Constants.ECS_RESOURCE,
                    region_name=self._region,
                )

            return self._ecs

        def _tags(self):
            return self._task_definition().get(
                Constants.TAGS,
            )

        def _task_definition(self):
            return self.ecs.describe_task_definition(
                taskDefinition=Constants.AWS_ECS_TASK_DEFINITION,
                include=[
                    'TAGS',
                ]
            )

    class EBService:
        def __init__(self, region):
            self._event_bridge_service = None
            self._region = region

        def put_events(self, entries):
            self.event_bridge_service.put_events(
                Entries=entries,
            )

        @property
        def event_bridge_service(self):
            if self._event_bridge_service is None:
                self._event_bridge_service = boto3.client(
                    Constants.EB_RESOURCE,
                    region_name=self._region,
                )

            return self._event_bridge_service

    class ESService:

        def __init__(self, region, session):
            self._domain = None
            self._http_authorization = None
            self._region = region

            self._es_service = None
            self._session = session
            self._ssm_service = AWSService().ssm_service

        def delete_by_query(self, query, index):
            return self.es_service.delete_by_query(
                index,
                query,
            )
        
        def get_index(self, index):
            return self.es_service.indices.get(index)
        
        def index(self, generator, *args):
            bulk(
                self.es_service,
                generator(*args),
            )
        
        def search(self, query, index):
            return self.es_service.search(
                query,
                index=index,
            )
        
        @property
        def domain(self):
            return self._domain

        @domain.setter
        def domain(self, value):
            self._domain = value
        
        @property
        def es_service(self):
            if self._es_service is None:
                self._es_service = Elasticsearch(
                    hosts=[
                        {
                            Constants.HOST: self.domain,
                            Constants.PORT: Constants.PORT_TLS,
                        }
                    ],
                    http_auth=self.http_authorization,
                    use_ssl=True,
                    verify_certs=True,
                    connection_class=RequestsHttpConnection,
                    timeout=Constants.ES_TIMEOUT,
                )
            
            return self._es_service

        @property
        def http_authorization(self):
            if self._http_authorization is None:
                credentials = self._session.get_credentials()
                self._http_authorization = AWS4Auth(
                    region=self._region,
                    service=Constants.ES_RESOURCE,
                    refreshable_credentials=credentials,
                )
            
            return self._http_authorization

    # TODO(declan.ryan@getvisibly.com) Pass `stacklevel` as a (default) parameter
    # This helps when logging exceptions from async tasks (see `data-sync`).
    class LogService:

        def __init__(self, region):
            self._handler = None
            self._region = region
            self._service = None
            self._session = None

            self._ssm_service = AWSService().ssm_service

        def debug(self, value):
            self.service.debug(value, stacklevel=2)

        def error(self, value):
            self.service.error(value, stacklevel=2)

        def event(self, value):
            self.service.event(value, stacklevel=2)

        def exception(self, value):
            self.service.exception(value, stacklevel=2)

        def info(self, value):
            self.service.info(value, stacklevel=2)

        # TODO(declan.ryan@getvisibly.com) Align warning with warn
        def warning(self, value):
            self.service.warn(value, stacklevel=2)

        @property
        def delegate(self):
            return self._delegate

        @property
        def handler(self):
            if self._handler is None:
                self._handler = watchtower.CloudWatchLogHandler(
                    create_log_group=False,
                    log_group=self._ssm_service.cloudwatch_log_group,
                    stream_name=self._ssm_service.cloudwatch_log_stream,
                    use_queues=True,
                    boto3_session=self.session,
                )
                self._handler.setFormatter(
                    UTCFormatter(Constants.LOG_FORMAT),
                )

            return self._handler

        @handler.setter
        def handler(self, value):
            self._handler = value
            self._handler.setFormatter(
                    UTCFormatter(Constants.LOG_FORMAT),
                )

        @property
        def service(self):
            if self._service is None:
                self._add_custom_levels()
                self._service = logging.getLogger(
                    self._ssm_service.cloudwatch_log_namespace,
                )
                self._service.addHandler(
                    self.handler,
                )
                self._service.setLevel(
                    logging.DEBUG,
                )

            return self._service

        @property
        def session(self):
            if self._session is None:
                self._session = boto3.session.Session(
                    region_name=self._region,
                )

            return self._session

        def _add_custom_levels(self):
            logging.EVENT = 51
            logging.addLevelName(
                logging.EVENT,
                Constants.EVENT,
            )

            def handler(self, msg, *args, **kwargs):
                self._log(logging.EVENT, msg, args, **kwargs)

            logging.Logger.event = handler

    class S3Service:

        def __init__(self):
            self._s3_service = None

        def download(self, key, bucket):
            file_as_bytes = BytesIO()
            self.s3_service.download_fileobj(
                bucket,
                key,
                file_as_bytes,
            )
            file_as_bytes.seek(0)

            return file_as_bytes

        @property
        def s3_service(self):
            if self._s3_service is None:
                self._s3_service = boto3.client(
                    Constants.S3_RESOURCE,
                )

            return self._s3_service

    class SharedSSMService:

        def __init__(self, delegate):
            self._delegate = delegate
            self._region = 'us-west-1'
            self._ssm = None
            
        def get(self, name, with_decryption=True):
            response = self.ssm.get_parameter(
                Name=name,
                WithDecryption=with_decryption,
            )
            self._ssm = None  # Forces to access latest STS session via delegate
            return response.get(
                Constants.PARAMETER_KEY,
            ).get(
                Constants.VALUE_KEY,
            )

        def put(self, name, value, _type='SecureString'):
            self.ssm.put_parameter(
                Name=name,
                Overwrite=True,
                Type=_type,
                Value=value,
            )
            self._ssm = None

        @property
        def amazon_aa_access_token(self):
            return self.get(
                'shared-ssm-amazon-aa-sa-access-token-parameter',
            )

        @property
        def delegate(self):
            return self._delegate

        @property
        def ssm(self):
            if self._ssm is None:
                self._ssm = self.delegate.refreshable_session.client(
                    Constants.SSM_RESOURCE,
                    region_name=self._region,
                )
            
            return self._ssm
                
    class SSMService:

        def __init__(self, prefix, region):
            self._prefix = prefix
            self._region = region
            self._ssm = None

            self._access_token_expire_minutes = None
            self._amazon_aa_client_id = None
            self._amazon_aa_client_secret = None
            self._amazon_advertising_elasticsearch_domain = None
            self._amazon_retail_elasticsearch_domain = None
            self._api_prefix = None
            self._application_secret = None
            self._cloudwatch_log_namespace = None
            self._cloudwatch_log_stream = None
            self._cloudwatch_log_group = None
            self._bucket = None
            self._cookie_domain = None
            self._documentdb_endpoint = None
            self._documentdb_password = None
            self._documentdb_port = None
            self._documentdb_user = None
            self._encryption_key = None
            self._event_bus = None
            self._shared_parameter_store_role_arn = None
            self._twilio_account_sid = None
            self._twilio_auth_token = None
            self._twilio_confirmation_template = None
            self._twilio_email_config_service = None
            self._twilio_invitation_template = None
            self._twilio_reset_password_template = None
            self._uri = None

        def get(self, name, with_decryption=True):
            response = self.ssm.get_parameter(
                Name=name,
                WithDecryption=with_decryption,
            )
            self._ssm = None  # Forces to access latest STS session via delegate
            return response.get(
                Constants.PARAMETER_KEY,
            ).get(
                Constants.VALUE_KEY,
            )

        def put(self, name, value, _type='SecureString'):
            self.ssm.put_parameter(
                Name=name,
                Overwrite=True,
                Type=_type,
                Value=value,
            )
            self._ssm = None

        @property
        def access_token_expire_minutes(self):
            if self._access_token_expire_minutes is None:
                self._access_token_expire_minutes = self.get(
                    f'{self.prefix}-ssm-visibly-server-access-token-expire-minutes-parameter',
                )

            return self._access_token_expire_minutes
        
        @property
        def amazon_aa_client_id(self):
            if self._amazon_aa_client_id is None:
                self._amazon_aa_client_id = self.get(
                    f'{self.prefix}-ssm-visibly-server-amazon-aa-client-id-parameter',
                )

            return self._amazon_aa_client_id
        
        @property
        def amazon_aa_client_secret(self):
            if self._amazon_aa_client_secret is None:
                self._amazon_aa_client_secret = self.get(
                    f'{self.prefix}-ssm-visibly-server-amazon-aa-client-secret-parameter',
                )
            
            return self._amazon_aa_client_secret
        
        @property
        def shared_parameter_store_role_arn(self):
            if self._shared_parameter_store_role_arn is None:
                self._shared_parameter_store_role_arn = self.get(
                    f'{self.prefix}-ssm-visibly-server-amazon-aa-parameter-store-role-arn-parameter',
                )

            return self._shared_parameter_store_role_arn
        
        @property
        def amazon_advertising_elasticsearch_domain(self):
            if self._amazon_advertising_elasticsearch_domain is None:
                self._amazon_advertising_elasticsearch_domain = self.get(
                    f'{self.prefix}-ssm-visibly-server-amazon-advertising-elasticsearch-domain-parameter',
                )

            return self._amazon_advertising_elasticsearch_domain
        
        @property
        def amazon_retail_elasticsearch_domain(self):
            if self._amazon_retail_elasticsearch_domain is None:
                self._amazon_retail_elasticsearch_domain = self.get(
                    f'{self.prefix}-ssm-visibly-server-amazon-retail-elasticsearch-domain-parameter',
                )

            return self._amazon_retail_elasticsearch_domain
        
        @property
        def api_prefix(self):
            # TODO Add to SSM Parameter Store
            return '/api/v1'

        @property
        def application_secret(self):
            if self._application_secret is None:
                self._application_secret = self.get(
                    f'{self.prefix}-ssm-visibly-server-application-secret-parameter',
                )

            return self._application_secret
        
        @property
        def bucket(self):
            if self._bucket is None:
                self._bucket = self.get(
                    f'{self.prefix}-ssm-visibly-server-s3-bucket-parameter',
                )
            
            return self._bucket
        
        @property
        def cloudwatch_log_group(self):
            if self._cloudwatch_log_group is None:
                self._cloudwatch_log_group = self.get(
                    f'{self.prefix}-ssm-visibly-server-cloudwatch-log-group-parameter'
                )

            return self._cloudwatch_log_group

        @property
        def cloudwatch_log_namespace(self):
            if self._cloudwatch_log_namespace is None:
                self._cloudwatch_log_namespace = self.get(
                    f'{self.prefix}-ssm-visibly-server-cloudwatch-log-namespace-parameter'
                )

            return self._cloudwatch_log_namespace

        @property
        def cloudwatch_log_stream(self):
            if self._cloudwatch_log_stream is None:
                self._cloudwatch_log_stream = self.get(
                    f'{self.prefix}-ssm-visibly-server-cloudwatch-log-stream-parameter'
                )

            return self._cloudwatch_log_stream

        @property
        def cookie_domain(self):
            if self._cookie_domain is None:
                self._cookie_domain = self.get(
                    f'{self.prefix}-ssm-visibly-server-cookie-domain-parameter',
                )
            
            return self._cookie_domain
        
        @property
        def documentdb_endpoint(self):
            if self._documentdb_endpoint is None:
                self._documentdb_endpoint = self.get(
                    f'{self.prefix}-ssm-visibly-server-documentdb-endpoint-parameter',
                )
            
            return self._documentdb_endpoint
        
        @property
        def documentdb_password(self):
            if self._documentdb_password is None:
                self._documentdb_password = self.get(
                    f'{self.prefix}-ssm-visibly-server-documentdb-password-parameter',
                )

            return self._documentdb_password
        
        @property
        def documentdb_port(self):
            if self._documentdb_port is None:
                self._documentdb_port = self.get(
                    f'{self.prefix}-ssm-visibly-server-documentdb-port-parameter',
                )

            return self._documentdb_port
        
        @property
        def documentdb_user(self):
            if self._documentdb_user is None:
                self._documentdb_user = self.get(
                    f'{self.prefix}-ssm-visibly-server-documentdb-user-parameter',
                )

            return self._documentdb_user
        
        @property
        def encryption_key(self):
            if self._encryption_key is None:
                self._encryption_key = self.get(
                    f'{self.prefix}-ssm-visibly-server-encryption-key-parameter',
                )

            return self._encryption_key
        
        @property
        def event_bus(self):
            if self._event_bus is None:
                self._event_bus = self.get(
                    f'{self.prefix}-ssm-visibly-server-event-bus-parameter'
                )

            return self._event_bus

        @property
        def prefix(self):
            return self._prefix

        @property
        def shared_parameter_store_role_arn(self):
            if self._shared_parameter_store_role_arn is None:
                self._shared_parameter_store_role_arn = self.get(
                    f'{self.prefix}-ssm-visibly-server-shared-parameter-store-role-arn-parameter',
                )

            return self._shared_parameter_store_role_arn
        
        @property
        def ssm(self):
            if self._ssm is None:
                self._ssm = boto3.client(
                    Constants.SSM_RESOURCE,
                    region_name=self._region,
                )

            return self._ssm

        @property
        def twilio_account_sid(self):
            if self._twilio_account_sid is None:
                self._twilio_account_sid = self.get(
                    f'{self.prefix}-ssm-visibly-server-twilio-account-sid-parameter',
                )
            
            return self._twilio_account_sid
        
        @property
        def twilio_auth_token(self):
            if self._twilio_auth_token is None:
                self._twilio_auth_token = self.get(
                    f'{self.prefix}-ssm-visibly-server-twilio-auth-token-parameter',
                )
            
            return self._twilio_auth_token
        
        @property
        def twilio_confirmation_template(self):
            if self._twilio_confirmation_template is None:
                self._twilio_confirmation_template = self.get(
                    f'{self.prefix}-ssm-visibly-server-twilio-confirm-template-id-parameter',
                )

            return self._twilio_confirmation_template
        
        @property
        def twilio_email_config_service(self):
            if self._twilio_email_config_service is None:
                self._twilio_email_config_service = self.get(
                    f'{self.prefix}-ssm-visibly-server-twilio-email-config-service-parameter',
                )
            
            return self._twilio_email_config_service
        
        @property
        def twilio_invitation_template(self):
            if self._twilio_invitation_template is None:
                self._twilio_invitation_template = self.get(
                    f'{self.prefix}-ssm-visibly-server-twilio-invite-template-id-parameter',
                )

            return self._twilio_invitation_template
        
        @property
        def twilio_reset_password_template(self):
            if self._twilio_reset_password_template is None:
                self._twilio_reset_password_template = self.get(
                    f'{self.prefix}-ssm-visibly-server-twilio-reset-template-id-parameter',
                )

            return self._twilio_reset_password_template
        
        @property
        def uri(self):
            if self._uri is None:
                self._uri = self.get(
                    f'{self.prefix}-ssm-visibly-server-s3-origin-parameter',
                )

            return self._uri
    
    class STSService:

        def __init__(self, region, role_arn, session_name, session):
            self._client = None
            self._region = region
            self._role_arn = role_arn
            self._refreshable_session = None
            self._session = session

        def assume_role(self):
            response = self.client.assume_role(
                RoleArn=self._role_arn,
                RoleSessionName='data-sync-assumed-role-session',
                DurationSeconds=1*60*60,
            )

            assumed_credentials = response.get('Credentials')
            if assumed_credentials is not None:
                return {
                    'access_key': assumed_credentials.get('AccessKeyId'),
                    'secret_key': assumed_credentials.get('SecretAccessKey'),
                    'token': assumed_credentials.get('SessionToken'),
                    'expiry_time': assumed_credentials.get('Expiration').isoformat(),
                }
            
            return {}

        @property
        def client(self):
            if self._client is None:
                self._client = self._session.client(
                    Constants.STS_RESOURCE,
                    region_name=self._region,
                )
            
            return self._client
            
        @property
        def refreshable_session(self):
            if self._refreshable_session is None:
                refreshable_credentials = RefreshableCredentials.create_from_metadata(
                    metadata=self.assume_role(),
                    refresh_using=self.assume_role,
                    method='sts-assume-role',
                )

                session = get_session()
                session._credentials = refreshable_credentials
                session.set_config_variable(
                    'region',
                    'us-west-1',
                )
                self._refreshable_session = boto3.Session(botocore_session=session)

            return self._refreshable_session
            
    __docdb_service = None
    __log_service = None
    __shared_ssm_service = None
    __ssm_service = None

    def __init__(self):
        self._advertiser_name = None
        self._region = None

        self._eb_service = None
        self._ecs_service = None
        self._es_service = None
        self._s3_service = None
        self._session = None
        self._ssm_service = None
        self._sts_service = None

    @property
    def advertiser_name(self):
        if self._advertiser_name is None:
            if settings.ADVERTISER_NAME is None:
                self._advertiser_name = self.ecs_service.tag_value_for_key(
                    Constants.ADVERTISER_NAME,
                )
            else:
                self._advertiser_name = settings.ADVERTISER_NAME

        return self._advertiser_name

    @property
    def docdb_service(self):
        if AWSService.__docdb_service is None:
            AWSService.__docdb_service = AWSService.DocumentDBService(
                self.ssm_service,
            )

        return AWSService.__docdb_service
    
    @property
    def eb_service(self):
        if self._eb_service is None:
            self._eb_service = AWSService.EBService(
                region=self.region,
            )

        return self._eb_service

    @property
    def ecs_service(self):
        if self._ecs_service is None:
            self._ecs_service = AWSService.ECSService(
                region=self.region,
            )

        return self._ecs_service

    @property
    def es_service(self):
        if self._es_service is None:
            self._es_service = AWSService.ESService(
                region=self.region,
                session=self.session,
            )

        return self._es_service

    @property
    def log_service(self):
        if AWSService.__log_service is None:
            AWSService.__log_service = AWSService.LogService(
                region=self.region,
            )

        return AWSService.__log_service

    @property
    def region(self):
        if self._region is None:
            self._region = settings.AMAZON_WEB_SERVICES_REGION

        return self._region

    @property
    def s3_service(self):
        if self._s3_service is None:
            self._s3_service = AWSService.S3Service()

        return self._s3_service

    @property
    def session(self):
        if self._session is None:
            self._session = boto3.Session()

        return self._session

    @property
    def shared_ssm_service(self):
        if AWSService.__shared_ssm_service is None:
            AWSService.__shared_ssm_service = AWSService.SharedSSMService(
                self.sts_service,
            )

        return AWSService.__shared_ssm_service

    @property
    def ssm_service(self):
        if AWSService.__ssm_service is None:
            AWSService.__ssm_service = AWSService.SSMService(
                prefix=self.advertiser_name,
                region=self.region,
            )

        return AWSService.__ssm_service

    @property
    def sts_service(self):
        if self._sts_service is None:
            self._sts_service = AWSService.STSService(
                region=self.region,
                role_arn=self.ssm_service.shared_parameter_store_role_arn,
                session_name=Constants.STS_SESSION_NAME,
                session=self.session,
            )

        return self._sts_service
