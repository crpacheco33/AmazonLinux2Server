from datetime import timedelta

import os
import time
import uuid

from httpx import AsyncClient

import pymongo
import pytest

from amazon_api.resources.aa.client import Client
from starlette.testclient import TestClient
from testcontainers.mongodb import MongoDbContainer

from server.core.constants import Constants
from server.delegates.aa_delegate import AADelegate
from server.dependencies import Interface
from server.managers.brand_manager import BrandManager
from server.resources.models.brand import Brand
from server.resources.models.user import User
from server.resources.schema.token import (
    AccessToken,
    RefreshToken,
)
from server.resources.schema.user import (
    UserInvitationSchema,
)
from server.resources.types.data_types import UserStatusType
from server.services.auth_service import AuthService
from server.services.aws_service import AWSService
from server.services.data_service import DataService
from server.services.twilio_service import TwilioService
from server.services.user_service import UserService
from server.utilities.aa_utility import AAUtility
from server.utilities.auth_utility import AuthUtility
from server.utilities.data_utility import DataUtility
from server.utilities.date_utility import DateUtility

from tests.mocks.interface_mock import InterfaceMock
from tests.test_constants import TestConstants


@pytest.fixture()
def ssm_service(mocker):
    is_bitbucket = os.getenv('BITBUCKET_ENV')
    if is_bitbucket:
        mocker.patch(
            'server.dependencies.Interface.index',
            return_value=[],
        )
        mocker.patch(
            'server.dependencies.Interface.show',
            return_value=[],
        )
    
    return AWSService.SSMService(
        prefix=TestConstants.TEST,
        region='us-west-1',
    )
    

@pytest.fixture()
def test_client():
    database_uri = os.getenv('TEST_DATABASE_URI')
    aws_service = AWSService()
    if database_uri:
        docdb_service = aws_service.docdb_service
        client = docdb_service.client

        client.visibly.brands.create_index(
            [( 'name', pymongo.DESCENDING, )],
            unique=True,
        )

        brands = _brands()

        Brand.create_many(brands, client)

        yield client
        client.drop_database('visibly')
    else:
        with MongoDbContainer('mongo:latest') as mongo:
            client = mongo.get_connection_client()

            client.visibly.brands.create_index(
                [( 'name', pymongo.DESCENDING, )],
                unique=True,
            )

            brands = _brands()

            Brand.create_many(brands, client)

            class DocumentDBServiceMock:

                def __init__(self, client):
                    self._client = client

                @property
                def client(self):
                    return self._client


            # token_utility.py (a circular dependency of dependencies.py) cannot
            # have its dependency on 'client' stubbed. This is a workaround to
            # enable local testing.
            docdb_service_mock = DocumentDBServiceMock(client)
            AWSService._AWSService__docdb_service = docdb_service_mock

            yield client


@pytest.fixture()
def sandbox_brand(test_client):
    brand = Brand.find_by_name(
        TestConstants.SANDBOX_BRAND_NAME,
        test_client,
    )

    brand_manager = BrandManager()
    brand_manager.brand = brand

    return brand


@pytest.fixture()
def test_brand(test_client):
    brand = Brand.find_by_name(
        TestConstants.BRAND_NAME,
        test_client,
    )

    brand_manager = BrandManager()
    brand_manager.brand = brand

    return brand


@pytest.fixture()
def auth_service(ssm_service, twilio_service):
    return AuthService(
        ssm_service,
        twilio_service,
    )


@pytest.fixture()
def auth_utility(mocker):
    mocker.patch(
        'server.utilities.auth_utility.AuthUtility.password',
        return_value=TestConstants.PASSWORD,
    )
    
    return AuthUtility()


@pytest.fixture()
def aws_service():
    return AWSService()


@pytest.fixture()
def advertising_domain(aws_service):
    es_service = aws_service.es_service
    es_service.domain = aws_service.ssm_service.amazon_advertising_elasticsearch_domain

    return DataService(es_service.es_service)


@pytest.fixture()
def data_utility():
    return DataUtility()


@pytest.fixture()
def date_utility():
    return DateUtility()


@pytest.fixture()
def profiles_client(aws_service, ssm_service):
    aa_utility = AAUtility()
    
    shared_ssm_service = aws_service.shared_ssm_service
    
    region = TestConstants.TEST_REGION
    version = aa_utility.version_for_api(
        Constants.PROFILES,
    )
    aa_delegate = AADelegate(
        Constants.PROFILES,
        region,
        version,
        ssm_service,
        shared_ssm_service,
    )

    return Client(aa_delegate)


@pytest.mark.asyncio
@pytest.fixture()
async def advertiser(profiles_interface):
    return await profiles_interface.register_brand(
        TestConstants.BRAND_NAME,
    )


@pytest.fixture()
def profiles_interface(profiles_client):
    klass = AAUtility().profile_interface_klass()
    return Interface(
        profiles_client,
        klass,
    )


@pytest.fixture()
def admin_user(auth_service, auth_utility, ssm_service, test_client):
    auth_service.invite(
        TestConstants.ADMIN_EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE, TestConstants.ADMIN],
        [TestConstants.BRAND_NAME],
        test_client,
    )

    expected = UserStatusType.PENDING
    
    existing_user = User.find_by_email(
        TestConstants.ADMIN_EMAIL,
        test_client,
    )
    actual = existing_user.status

    assert expected == actual

    invitation = {
        Constants.DATA: auth_utility.encrypt_parameters(
            {
                Constants.PASS: existing_user.password,
                Constants.BRANDS: [TestConstants.BRAND_NAME],
            },
            ssm_service.encryption_key,
        ),
        Constants.EMAIL: TestConstants.ADMIN_EMAIL,
        Constants.TS: int(time.time()),
    }

    hashed_invitation = auth_utility.encrypt_parameters(
        invitation,
        ssm_service.encryption_key,
    )
    
    auth_service.register(
        UserInvitationSchema(
            hash=hashed_invitation,
            password=TestConstants.PASSWORD,
        ),
        test_client,
    )

    return User.find_by_email(
        TestConstants.ADMIN_EMAIL,
        test_client,
    )


@pytest.fixture()
def admin_user_access_token(auth_utility, admin_user, ssm_service, test_client):
    brand = Brand.find_by_name(
        TestConstants.BRAND_NAME,
        test_client,
    )
    
    access_token = AccessToken(
        id=str(admin_user._id),
        email=admin_user.email,
        full_name=admin_user.full_name,
        scopes=[scope for scope in admin_user.scopes],
        brand=str(brand._id),
    )

    return auth_utility.encode_token(
        access_token,
        timedelta(days=1),
        ssm_service.application_secret,
    )[0]


@pytest.fixture()
def read_only_user(auth_service, auth_utility, ssm_service, test_client):
    auth_service.invite(
        TestConstants.READ_ONLY_EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ],
        [TestConstants.BRAND_NAME],
        test_client,
    )

    expected = UserStatusType.PENDING
    
    existing_user = User.find_by_email(
        TestConstants.READ_ONLY_EMAIL,
        test_client,
    )
    actual = existing_user.status

    assert expected == actual

    invitation = {
        Constants.DATA: auth_utility.encrypt_parameters(
            {
                Constants.PASS: existing_user.password,
                Constants.BRANDS: [TestConstants.BRAND_NAME],
            },
            ssm_service.encryption_key,
        ),
        Constants.EMAIL: TestConstants.READ_ONLY_EMAIL,
        Constants.TS: int(time.time()),
    }

    hashed_invitation = auth_utility.encrypt_parameters(
        invitation,
        ssm_service.encryption_key,
    )
    
    auth_service.register(
        UserInvitationSchema(
            hash=hashed_invitation,
            password=TestConstants.PASSWORD,
        ),
        test_client,
    )

    return User.find_by_email(
        TestConstants.READ_ONLY_EMAIL,
        test_client,
    )


@pytest.fixture()
def read_only_user_access_token(auth_utility, read_only_user, ssm_service, test_client):
    brand = Brand.find_by_name(
        TestConstants.BRAND_NAME,
        test_client,
    )

    access_token = AccessToken(
        id=str(read_only_user._id),
        email=read_only_user.email,
        full_name=read_only_user.full_name,
        scopes=[scope for scope in read_only_user.scopes],
        brand=str(brand._id),
    )

    return auth_utility.encode_token(
        access_token,
        timedelta(days=1),
        ssm_service.application_secret,
    )[0]


@pytest.fixture()
def read_write_user(auth_service, auth_utility, ssm_service, test_client):
    auth_service.invite(
        TestConstants.EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
        [TestConstants.BRAND_NAME, TestConstants.SANDBOX_BRAND_NAME],
        test_client,
    )

    expected = UserStatusType.PENDING
    
    existing_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )
    actual = existing_user.status

    assert expected == actual

    invitation = {
        Constants.DATA: auth_utility.encrypt_parameters(
            {
                Constants.PASS: existing_user.password,
                Constants.BRANDS: [TestConstants.BRAND_NAME, TestConstants.SANDBOX_BRAND_NAME],
            },
            ssm_service.encryption_key,
        ),
        Constants.EMAIL: TestConstants.EMAIL,
        Constants.TS: int(time.time()),
    }

    hashed_invitation = auth_utility.encrypt_parameters(
        invitation,
        ssm_service.encryption_key,
    )
    
    auth_service.register(
        UserInvitationSchema(
            hash=hashed_invitation,
            password=TestConstants.PASSWORD,
        ),
        test_client,
    )

    user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    return User.update(
        user._id,
        {
            '$set': {
                'refresh_token': str(uuid.uuid4()),
            }
        },
        test_client,
    )


@pytest.fixture()
def read_write_user_access_token(auth_utility, read_write_user, ssm_service, test_client):
    brand = Brand.find_by_name(
        TestConstants.BRAND_NAME,
        test_client,
    )
    
    access_token = AccessToken(
        id=str(read_write_user._id),
        email=read_write_user.email,
        full_name=read_write_user.full_name,
        scopes=[scope for scope in read_write_user.scopes],
        brand=str(brand._id),
    )

    return auth_utility.encode_token(
        access_token,
        timedelta(days=1),
        ssm_service.application_secret,
    )[0]


@pytest.fixture()
def read_write_user_alternative(auth_service, auth_utility, ssm_service, test_client):
    auth_service.invite(
        TestConstants.ALTERNATIVE_BRAND_EMAIL,
        TestConstants.ALTERNATIVE_FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
        [TestConstants.BRAND_NAME],
        test_client,
    )

    expected = UserStatusType.PENDING
    
    existing_user = User.find_by_email(
        TestConstants.ALTERNATIVE_BRAND_EMAIL,
        test_client,
    )
    actual = existing_user.status

    assert expected == actual

    invitation = {
        Constants.DATA: auth_utility.encrypt_parameters(
            {
                Constants.PASS: existing_user.password,
                Constants.BRANDS: [TestConstants.BRAND_NAME],
            },
            ssm_service.encryption_key,
        ),
        Constants.EMAIL: TestConstants.ALTERNATIVE_BRAND_EMAIL,
        Constants.TS: int(time.time()),
    }

    hashed_invitation = auth_utility.encrypt_parameters(
        invitation,
        ssm_service.encryption_key,
    )
    
    auth_service.register(
        UserInvitationSchema(
            hash=hashed_invitation,
            password=TestConstants.PASSWORD,
        ),
        test_client,
    )

    return User.find_by_email(
        TestConstants.ALTERNATIVE_BRAND_EMAIL,
        test_client,
    )


@pytest.fixture()
def read_write_user_access_token_alternative(auth_utility, read_write_user_alternative, ssm_service, test_client):
    brand = Brand.find_by_name(
        TestConstants.ALTERNATIVE_BRAND_NAME,
        test_client,
    )
    
    access_token = AccessToken(
        id=str(read_write_user_alternative._id),
        email=read_write_user_alternative.email,
        full_name=read_write_user_alternative.full_name,
        scopes=[scope for scope in read_write_user_alternative.scopes],
        brand=str(brand._id),
    )

    return auth_utility.encode_token(
        access_token,
        timedelta(days=1),
        ssm_service.application_secret,
    )[0]


@pytest.fixture()
def read_write_user_same_account(auth_service, auth_utility, ssm_service, test_client):
    auth_service.invite(
        TestConstants.ALTERNATIVE_EMAIL,
        TestConstants.ALTERNATIVE_FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
        [TestConstants.BRAND_NAME],
        test_client,
    )

    expected = UserStatusType.PENDING
    
    existing_user = User.find_by_email(
        TestConstants.ALTERNATIVE_EMAIL,
        test_client,
    )
    actual = existing_user.status

    assert expected == actual

    invitation = {
        Constants.DATA: auth_utility.encrypt_parameters(
            {
                Constants.PASS: existing_user.password,
                Constants.BRANDS: [TestConstants.BRAND_NAME],
            },
            ssm_service.encryption_key,
        ),
        Constants.EMAIL: TestConstants.ALTERNATIVE_EMAIL,
        Constants.TS: int(time.time()),
    }

    hashed_invitation = auth_utility.encrypt_parameters(
        invitation,
        ssm_service.encryption_key,
    )
    
    auth_service.register(
        UserInvitationSchema(
            hash=hashed_invitation,
            password=TestConstants.PASSWORD,
        ),
        test_client,
    )

    return User.find_by_email(
        TestConstants.ALTERNATIVE_EMAIL,
        test_client,
    )


@pytest.fixture()
def read_write_user_access_token_same_account(auth_utility, read_write_user_alternative, ssm_service, test_client):
    brand = Brand.find_by_name(
        TestConstants.BRAND_NAME,
        test_client,
    )
    
    access_token = AccessToken(
        id=str(read_write_user_alternative._id),
        email=read_write_user_alternative.email,
        full_name=read_write_user_alternative.full_name,
        scopes=[scope for scope in read_write_user_alternative.scopes],
        brand=str(brand._id),
    )

    return auth_utility.encode_token(
        access_token,
        timedelta(days=1),
        ssm_service.application_secret,
    )[0]


@pytest.fixture()
def read_write_user_refresh_token(auth_utility, read_write_user, ssm_service, test_brand):
    refresh_token_details = RefreshToken(
        brand_id=str(test_brand._id),
        email=read_write_user.email,
        version=str(read_write_user.refresh_token),
    )
    refresh_token, _ = auth_utility.encode_token(
        refresh_token_details,
        timedelta(days=1),
        ssm_service.application_secret,
    )
    return refresh_token


@pytest.fixture()
def sd_test_client(aws_service, ssm_service):
    aa_utility = AAUtility()
    
    shared_ssm_service = aws_service.shared_ssm_service
    
    region = TestConstants.TEST_REGION
    version = aa_utility.version_for_api(
        Constants.SPONSORED_DISPLAY,
    )
    aa_delegate = AADelegate(
        Constants.SPONSORED_DISPLAY,
        region,
        version,
        ssm_service,
        shared_ssm_service,
    )

    return Client(aa_delegate)


@pytest.fixture()
def sd_ad_group_interface(sd_test_client):
    klass = AAUtility().ad_group_interface_klass(
        Constants.SPONSORED_DISPLAY,
    )
    return Interface(
        sd_test_client,
        klass,
    )


@pytest.fixture()
def sd_campaign_interface(sd_test_client):
    klass = AAUtility().campaign_interface_klass(
        Constants.SPONSORED_DISPLAY,
    )
    return Interface(
        sd_test_client,
        klass,
    )


@pytest.fixture()
def sd_product_ad_interface(sd_test_client):
    klass = AAUtility().product_ad_interface_klass(
        Constants.SPONSORED_DISPLAY,
    )
    return Interface(
        sd_test_client,
        klass,
    )


@pytest.fixture()
def sd_target_interface(sd_test_client):
    klass = AAUtility().target_interface_klass(
        Constants.SPONSORED_DISPLAY,
    )
    return Interface(
        sd_test_client,
        klass,
    )


@pytest.fixture()
def shared_read_write_user(auth_service, auth_utility, ssm_service, test_client):
    brands = [
        f'{TestConstants.BRAND_NAME} 1',
        f'{TestConstants.BRAND_NAME} 2',
        f'{TestConstants.BRAND_NAME} 3',
    ]
    
    auth_service.invite(
        TestConstants.EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
        brands,
        test_client,
    )

    expected = UserStatusType.PENDING
    
    existing_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )
    actual = existing_user.status

    assert expected == actual

    invitation = {
        Constants.DATA: auth_utility.encrypt_parameters(
            {
                Constants.PASS: existing_user.password,
                Constants.BRANDS: brands,
            },
            ssm_service.encryption_key,
        ),
        Constants.EMAIL: TestConstants.EMAIL,
        Constants.TS: int(time.time()),
    }

    hashed_invitation = auth_utility.encrypt_parameters(
        invitation,
        ssm_service.encryption_key,
    )
    
    auth_service.register(
        UserInvitationSchema(
            hash=hashed_invitation,
            password=TestConstants.PASSWORD,
        ),
        test_client,
    )

    return User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )


@pytest.fixture()
def shared_read_write_user_access_token(auth_utility, shared_read_write_user, ssm_service, test_client):
    brand = Brand.find_by_name(
        f'{TestConstants.BRAND_NAME} 1',
        test_client,
    )
    
    access_token = AccessToken(
        id=str(shared_read_write_user._id),
        email=shared_read_write_user.email,
        full_name=shared_read_write_user.full_name,
        scopes=[scope for scope in shared_read_write_user.scopes],
        brand=str(brand._id),
    )

    return auth_utility.encode_token(
        access_token,
        timedelta(days=1),
        ssm_service.application_secret,
    )[0]


@pytest.fixture()
def sp_test_client(aws_service, ssm_service):
    aa_utility = AAUtility()
    
    shared_ssm_service = aws_service.shared_ssm_service
    
    region = TestConstants.TEST_REGION
    version = aa_utility.version_for_api(
        Constants.SPONSORED_PRODUCTS,
    )
    aa_delegate = AADelegate(
        Constants.SPONSORED_PRODUCTS,
        region,
        version,
        ssm_service,
        shared_ssm_service,
    )

    return Client(aa_delegate)


@pytest.fixture()
def sp_ad_group_interface(sp_test_client):
    klass = AAUtility().ad_group_interface_klass(
        Constants.SPONSORED_PRODUCTS,
    )
    return Interface(
        sp_test_client,
        klass,
    )


@pytest.fixture()
def sp_campaign_interface(sp_test_client):
    klass = AAUtility().campaign_interface_klass(
        Constants.SPONSORED_PRODUCTS,
    )
    return Interface(
        sp_test_client,
        klass,
    )


@pytest.fixture()
def sp_keyword_interface(sp_test_client):
    klass = AAUtility().keyword_interface_klass(
        Constants.SPONSORED_PRODUCTS,
    )
    return Interface(
        sp_test_client,
        klass,
    )


@pytest.fixture()
def sp_product_ad_interface(sp_test_client):
    klass = AAUtility().product_ad_interface_klass(
        Constants.SPONSORED_PRODUCTS,
    )
    return Interface(
        sp_test_client,
        klass,
    )


@pytest.fixture()
def sp_target_interface(sp_test_client):
    klass = AAUtility().target_interface_klass(
        Constants.SPONSORED_PRODUCTS,
    )
    return Interface(
        sp_test_client,
        klass,
    )


@pytest.fixture(autouse=True, scope='function')
def twilio_service(mocker, ssm_service):
    mocker.patch(
        'server.services.twilio_service.TwilioService.authenticate_two_factor',
        return_value=f'{ssm_service.uri}/#/auth/confirm?sid={TestConstants.SID}&code={TestConstants.CODE}',
    )
    mocker.patch(
        'server.services.twilio_service.TwilioService.email_confirmation',
        return_value=f'{ssm_service.uri}/#/auth/invite?hash={TestConstants.HASH}&code={TestConstants.CODE}',
    )
    mocker.patch(
        'server.services.twilio_service.TwilioService.wants_reset_password',
        return_value=f'{ssm_service.uri}/#/auth/invite?sid={TestConstants.SID}&code={TestConstants.CODE}',
    )

    return TwilioService()


@pytest.fixture()
def unregistered_user(auth_service, test_client):
    auth_service.invite(
        TestConstants.UNREGISTERED_EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
        [TestConstants.BRAND_NAME],
        test_client,
    )

    expected = UserStatusType.PENDING
    
    existing_user = User.find_by_email(
        TestConstants.UNREGISTERED_EMAIL,
        test_client,
    )
    actual = existing_user.status

    assert expected == actual

    return existing_user
    
    
@pytest.fixture()
def user_service(ssm_service, twilio_service):
    return UserService(
        ssm_service,
        twilio_service,
    )


@pytest.fixture()
def write_user(auth_service, auth_utility, ssm_service, test_client):
    auth_service.invite(
        TestConstants.EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.WRITE],
        [TestConstants.BRAND_NAME],
        test_client,
    )

    expected = UserStatusType.PENDING
    
    existing_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )
    actual = existing_user.status

    assert expected == actual

    invitation = {
        Constants.DATA: auth_utility.encrypt_parameters(
            {
                Constants.PASS: existing_user.password,
                Constants.BRANDS: [TestConstants.BRAND_NAME],
            },
            ssm_service.encryption_key,
        ),
        Constants.EMAIL: TestConstants.EMAIL,
        Constants.TS: int(time.time()),
    }

    hashed_invitation = auth_utility.encrypt_parameters(
        invitation,
        ssm_service.encryption_key,
    )
    
    auth_service.register(
        UserInvitationSchema(
            hash=hashed_invitation,
            password=TestConstants.PASSWORD,
        ),
        test_client,
    )

    return User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )


@pytest.fixture()
def write_user_access_token(auth_utility, write_user, ssm_service, test_client):
    brand = Brand.find_by_name(
        TestConstants.BRAND_NAME,
        test_client,
    )
    
    access_token = AccessToken(
        id=str(write_user._id),
        email=write_user.email,
        full_name=write_user.full_name,
        scopes=[scope for scope in write_user.scopes],
        brand=str(brand._id),
    )

    return auth_utility.encode_token(
        access_token,
        timedelta(days=1),
        ssm_service.application_secret,
    )[0]


def _brands():
    return [{
        'name': TestConstants.ALTERNATIVE_BRAND_NAME,
        'country': 'US',
        'currency': 'USD',
        'timezone': 'America/Los_Angeles',
        'amazon': {
            'aa': {
                'dsp': {
                    'advertiser_id': '4093311673971939',
                    'entity_id': '1191625552155416',
                    'name': 'DSP',
                },
                'sa': {
                    'advertiser_id': '4093311673971939',
                    'name': 'SA',
                },
                'region': 'na',
            }
        },
    },
    {
        'name': TestConstants.BRAND_NAME,
        'country': 'US',
        'currency': 'USD',
        'timezone': 'America/Los_Angeles',
        'amazon': {
            'aa': {
                'dsp': {
                    'advertiser_id': '2575637870401',
                    'entity_id': '1191625552155416',
                    'name': 'DSP',
                },
                'sa': {
                    'advertiser_id': '1110250468092771',
                    'name': 'SA',
                },
                'region': 'na',
            }
        },
    },
    {
        'name': f'{TestConstants.BRAND_NAME} 1',
        'country': 'US',
        'currency': 'USD',
        'timezone': 'America/Los_Angeles',
        'amazon': {
            'aa': {
                'dsp': {
                    'advertiser_id': '5',
                    'entity_id': '100',
                    'name': 'DSP',
                },
                'sa': {
                    'advertiser_id': '6',
                    'name': 'SA',
                },
                'region': 'na',
            }
        },
    },
    {
        'name': f'{TestConstants.BRAND_NAME} 2',
        'country': 'US',
        'currency': 'USD',
        'timezone': 'America/Los_Angeles',
        'amazon': {
            'aa': {
                'dsp': {
                    'advertiser_id': '7',
                    'entity_id': '100',
                    'name': 'DSP',
                },
                'sa': {
                    'advertiser_id': '8',
                    'name': 'SA',
                },
                'region': 'na',
            }
        },
    },
    {
        'name': f'{TestConstants.BRAND_NAME} 3',
        'country': 'US',
        'currency': 'USD',
        'timezone': 'America/Los_Angeles',
        'amazon': {
            'aa': {
                'dsp': {
                    'advertiser_id': '9',
                    'entity_id': '100',
                    'name': 'DSP',
                },
                'sa': {
                    'advertiser_id': '10',
                    'name': 'SA',
                },
                'region': 'na',
            }
        },
    },
    {
        'name': TestConstants.SANDBOX_BRAND_NAME,
        'country': 'US',
        'currency': 'USD',
        'timezone': 'America/Los_Angeles',
        'amazon': {
            'aa': {
                'dsp': {
                    'advertiser_id': '4093311673971939',
                    'entity_id': '4093311673971939',
                    'name': 'DSP',
                },
                'sa': {
                    'advertiser_id': '4093311673971939',
                    'name': 'SA',
                },
                'region': 'test',
            }
        },
    }]