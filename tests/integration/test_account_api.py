import logging

import httpx
import pytest

from server.resources.models.user import User
from server.resources.schema.user import UserPasswordUpdateSchema
from tests.test_constants import TestConstants


log = logging.getLogger(TestConstants.SERVER_TESTS)
log.setLevel(
    logging.INFO,
)


@pytest.mark.asyncio
@pytest.mark.account
async def test_accounts_returns_200_when_password_is_updated(auth_utility, read_write_user, read_write_user_access_token, test_client):
    from server.dependencies import (
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = UserPasswordUpdateSchema(
        new_password=TestConstants.NEW_PASSWORD,
        password=TestConstants.PASSWORD,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.patch(
            f'/api/v1/account/{read_write_user._id}',
            json=data.dict(),
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    expected = True
    actual = auth_utility.authenticate_password(
        TestConstants.NEW_PASSWORD,
        user.password,
    )
    
    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.account
async def test_accounts_returns_400_when_user_is_not_authentic(auth_utility, read_write_user, read_write_user_access_token, test_client):
    from server.dependencies import (
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = UserPasswordUpdateSchema(
        new_password=TestConstants.NEW_PASSWORD,
        password=TestConstants.PASSWORD_FAKE,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.patch(
            f'/api/v1/account/{read_write_user._id}',
            json=data.dict(),
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )
    
    expected = 400
    actual = response.status_code

    assert expected == actual

    user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    expected = True
    actual = auth_utility.authenticate_password(
        TestConstants.PASSWORD,
        user.password,
    )
    
    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.account
async def test_accounts_returns_422_when_request_is_invalid(auth_utility, read_write_user, read_write_user_access_token, test_client):
    from server.dependencies import (
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = {
        'new_password': TestConstants.PASSWORD_MISSING_SYMBOL,
        'password': TestConstants.PASSWORD,
    }

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.patch(
            f'/api/v1/account/{read_write_user._id}',
            json=data,
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )
    
    expected = 422
    actual = response.status_code

    assert expected == actual

    user = User.find_by_email(
        TestConstants.EMAIL,
        test_client,
    )

    expected = True
    actual = auth_utility.authenticate_password(
        TestConstants.PASSWORD,
        user.password,
    )
    
    assert expected == actual
