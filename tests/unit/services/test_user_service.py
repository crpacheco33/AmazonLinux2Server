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
def test_add_adds_user(mocker, user_service, test_client):
    existing_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    assert existing_user is None

    user_service.add(
        TestConstants.EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
        test_client,
    )

    existing_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    assert existing_user

    expected = TestConstants.EMAIL
    actual = existing_user.get('email')

    assert expected == actual

    expected = TestConstants.FULL_NAME
    actual = existing_user.get('full_name')

    assert expected == actual

    expected = UserStatusType.PENDING
    actual = existing_user.get('status')

    assert expected == actual


@pytest.mark.service
def test_add_brands_adds_brands(user_service, test_client):
    user_service.add(
        TestConstants.EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
        test_client,
    )

    user_service.invite(
        TestConstants.EMAIL,
        [TestConstants.BRAND_NAME],
        test_client,
    )

    existing_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    brands = existing_user.brands

    expected = 1
    actual = len(brands)

    assert expected == actual

    brand = Brand.find_by_name(
        TestConstants.ALTERNATIVE_BRAND_NAME,
        test_client,
    )

    user_service.add_brands(
        str(existing_user._id),
        [str(brand._id)],
        test_client,
    )

    existing_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    brands = existing_user.brands

    expected = 2
    actual = len(brands)

    assert expected == actual


@pytest.mark.service
def test_add_brands_adds_user_to_brands(user_service, test_client):
    user_service.add(
        TestConstants.EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
        test_client,
    )

    user_service.invite(
        TestConstants.EMAIL,
        [TestConstants.BRAND_NAME],
        test_client,
    )

    existing_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    brand = Brand.find_by_name(
        TestConstants.ALTERNATIVE_BRAND_NAME,
        test_client,
    )

    brand_users = brand.users

    assert existing_user._id not in brand_users

    user_service.add_brands(
        existing_user._id,
        [brand._id],
        test_client,
    )

    existing_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    brand = Brand.find_by_name(
        TestConstants.ALTERNATIVE_BRAND_NAME,
        test_client,
    )

    brand_users = brand.users

    assert existing_user._id in brand_users


@pytest.mark.service
def test_invite_invites_user(mocker, user_service, test_client):
    user_service.add(
        TestConstants.EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
        test_client,
    )

    existing_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    assert existing_user

    brands = existing_user.brands

    expected = 0
    actual = len(brands)

    assert expected == actual

    user_service.invite(
        TestConstants.EMAIL,
        [TestConstants.BRAND_NAME],
        test_client,
    )

    existing_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    expected = UserStatusType.PENDING
    actual = existing_user.get('status')

    assert expected == actual

    brands = existing_user.brands

    expected = 1
    actual = len(brands)

    assert expected == actual


@pytest.mark.service
def test_remove_brands_removes_brands(user_service, test_client):
    user_service.add(
        TestConstants.EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
        test_client,
    )

    user_service.invite(
        TestConstants.EMAIL,
        [TestConstants.BRAND_NAME, TestConstants.ALTERNATIVE_BRAND_NAME],
        test_client,
    )

    existing_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    brand = Brand.find_by_name(
        TestConstants.ALTERNATIVE_BRAND_NAME,
        test_client,
    )

    brands = existing_user.brands

    expected = 2
    actual = len(brands)

    assert expected == actual

    user_service.remove_brands(
        str(existing_user._id),
        [str(brand._id)],
        test_client,
    )

    existing_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    brands = existing_user.brands

    expected = 1
    actual = len(brands)

    assert expected == actual


@pytest.mark.service
def test_remove_brands_removes_user_from_brands(user_service, test_client):
    user_service.add(
        TestConstants.EMAIL,
        TestConstants.FULL_NAME,
        [TestConstants.READ, TestConstants.WRITE],
        test_client,
    )

    user_service.invite(
        TestConstants.EMAIL,
        [TestConstants.BRAND_NAME, TestConstants.ALTERNATIVE_BRAND_NAME],
        test_client,
    )

    existing_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    brand = Brand.find_by_name(
        TestConstants.ALTERNATIVE_BRAND_NAME,
        test_client,
    )

    brand_users = brand.users

    assert existing_user._id in brand_users

    user_service.remove_brands(
        existing_user._id,
        [brand._id],
        test_client,
    )

    existing_user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    brand = Brand.find_by_name(
        TestConstants.ALTERNATIVE_BRAND_NAME,
        test_client,
    )

    brand_users = brand.users

    assert existing_user._id not in brand_users
