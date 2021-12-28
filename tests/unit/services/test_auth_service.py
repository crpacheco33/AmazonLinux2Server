import logging
import time

import pytest

from server.core.constants import Constants
from server.resources.models.brand import Brand
from server.resources.models.user import User
from server.resources.schema.user import (
    UserInvitationSchema,
)
from server.resources.types.data_types import UserStatusType

from tests.test_constants import TestConstants


log = logging.getLogger(TestConstants.SERVER_TESTS)
log.setLevel(
    logging.INFO,
)


@pytest.mark.service
def test_invite_invites_user(auth_service, test_client):
    invited_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    assert invited_user is None

    response = auth_service.invite(
        TestConstants.EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
        [TestConstants.BRAND_NAME],
        test_client,
    )

    invited_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    assert invited_user

    expected = TestConstants.EMAIL
    actual = invited_user.get('email')

    assert expected == actual

    expected = TestConstants.FULL_NAME
    actual = invited_user.get('full_name')

    assert expected == actual

    expected = UserStatusType.PENDING
    actual = invited_user.get('status')

    assert expected == actual


@pytest.mark.service
def test_invite_again_creates_user_once_only(auth_service, test_client):
    users = User.find(test_client)

    expected = 0
    actual = len(users)

    assert expected == actual

    response = auth_service.invite(
        TestConstants.EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
        [TestConstants.BRAND_NAME],
        test_client,
    )

    users = User.find(test_client)

    expected = 1
    actual = len(users)

    assert expected == actual

    response = auth_service.invite(
        TestConstants.EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
        [TestConstants.BRAND_NAME],
        test_client,
    )

    users = User.find(test_client)

    expected = 1
    actual = len(users)

    assert expected == actual

    expected = TestConstants.EMAIL
    actual = users[0].email

    assert expected == actual

    expected = TestConstants.FULL_NAME
    actual = users[0].full_name

    assert expected == actual

    expected = UserStatusType.PENDING
    actual = users[0].status

    assert expected == actual


@pytest.mark.service
def test_invite_again_does_not_affect_user_brands(auth_service, test_client):
    auth_service.invite(
        TestConstants.EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
        [TestConstants.BRAND_NAME],
        test_client,
    )

    invited_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    brands = invited_user.brands

    expected = 1
    actual = len(brands)

    assert expected == actual

    auth_service.invite(
        TestConstants.EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
        [TestConstants.BRAND_NAME],
        test_client,
    )

    invited_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    brands = invited_user.brands

    expected = 1
    actual = len(brands)

    assert expected == actual

    brand = Brand.find_by_id(
        brands[0],
        test_client,
    )

    expected = TestConstants.BRAND_NAME
    actual = brand.name

    assert expected == actual


@pytest.mark.service
def test_register_creates_user(auth_service, auth_utility, ssm_service, test_client):
    auth_service.invite(
        TestConstants.EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
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

    expected = UserStatusType.ACTIVE
    
    existing_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )
    actual = existing_user.status

    assert expected == actual


@pytest.mark.service
def test_register_does_not_create_user_when_user_exists(auth_service, auth_utility, ssm_service, test_client):
    auth_service.invite(
        TestConstants.EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
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
    
    expected = True
    actual = auth_service.register(
        UserInvitationSchema(
            hash=hashed_invitation,
            password=TestConstants.PASSWORD,
        ),
        test_client,
    )

    assert expected == actual

    expected = False
    actual = auth_service.register(
        UserInvitationSchema(
            hash=hashed_invitation,
            password=TestConstants.PASSWORD,
        ),
        test_client,
    )

    assert expected == actual
