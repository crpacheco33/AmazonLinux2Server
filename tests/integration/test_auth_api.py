import logging
import re
import time

import httpx
import pytest

from server.core.constants import Constants
from server.resources.models.user import User
from server.resources.schema.user import (
    PasswordWantsResetSchema,
    PasswordWillResetSchema,
    UserInvitationSchema,
    User2FASchema,
    UserLoginSchema,
)
from server.resources.types.data_types import UserStatusType
from tests.test_constants import TestConstants


log = logging.getLogger(TestConstants.SERVER_TESTS)
log.setLevel(
    logging.INFO,
)


@pytest.mark.asyncio
@pytest.mark.auth
async def test_invite_returns_200_when_invitation_is_valid(auth_service, auth_utility, ssm_service, test_client, unregistered_user):
    from server.dependencies import (
        auth_service,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client

    invitation = {
        Constants.DATA: auth_utility.encrypt_parameters(
            {
                Constants.PASS: unregistered_user.password,
                Constants.BRANDS: [TestConstants.BRAND_NAME],
            },
            ssm_service.encryption_key,
        ),
        Constants.EMAIL: unregistered_user.email,
        Constants.TS: int(time.time()),
    }

    hashed_invitation = auth_utility.encrypt_parameters(
        invitation,
        ssm_service.encryption_key,
    )
    
    data = UserInvitationSchema(
        hash=hashed_invitation,
        password=TestConstants.PASSWORD,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/auth/invite',
            json=data.dict(),
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    user = User.find_by_email(
        TestConstants.UNREGISTERED_EMAIL,
        test_client,
    )

    expected = UserStatusType.ACTIVE
    actual = user.status
    
    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.auth
async def test_invite_returns_400_when_invitation_is_invalid(auth_service, auth_utility, ssm_service, test_client, unregistered_user):
    from server.dependencies import (
        auth_service,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client

    invitation = {
        Constants.DATA: auth_utility.encrypt_parameters(
            {
                Constants.PASS: unregistered_user.password,
                Constants.BRANDS: [TestConstants.BRAND_NAME],
            },
            ssm_service.encryption_key,
        ),
        Constants.EMAIL: TestConstants.UNREGISTERED_EMAIL,
        Constants.TS: int(time.time()),
    }

    hashed_invitation = auth_utility.encrypt_parameters(
        invitation,
        ssm_service.encryption_key,
    )
    
    data = UserInvitationSchema(
        hash='hashedinvitation',
        password=TestConstants.PASSWORD,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/auth/invite',
            json=data.dict(),
        )

    expected = 400
    actual = response.status_code

    assert expected == actual

    user = User.find_by_email(
        TestConstants.UNREGISTERED_EMAIL,
        test_client,
    )

    expected = UserStatusType.PENDING
    actual = user.status
    
    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.auth
async def test_wants_reset_password_returns_200_when_user_exists(auth_service, read_write_user, test_client):
    from server.dependencies import (
        auth_service,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client

    data = PasswordWantsResetSchema(
        email=read_write_user.email,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/auth/reset',
            json=data.dict(),
        )

    expected = 200
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.auth
async def test_wants_reset_password_returns_400_when_user_does_not_exist(auth_service, test_client):
    from server.dependencies import (
        auth_service,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client

    data = PasswordWantsResetSchema(
        email=TestConstants.UNKNOWN_EMAIL,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/auth/reset',
            json=data.dict(),
        )

    expected = 400
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.auth
async def test_wants_reset_password_returns_400_when_user_exists_and_is_inactive(auth_service, read_write_user, ssm_service, test_client):
    from server.dependencies import (
        auth_service,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client

    with test_client.start_session() as session:
        collection = test_client.visibly.users
        collection.update_one(
            { 'email': read_write_user.email },
            { '$set': { 'status': UserStatusType.DISABLED } },
        )
    
    data = PasswordWantsResetSchema(
        email=read_write_user.email,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/auth/reset',
            json=data.dict(),
        )

    expected = 400
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.auth
async def test_will_reset_password_returns_200_when_sid_is_valid(mocker, auth_service, auth_utility, read_write_user, ssm_service, twilio_service, test_client):
    from server.dependencies import (
        auth_service,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client

    mocker.patch(
        'server.services.twilio_service.TwilioService.authenticate_email',
        return_value=True,
    )

    sid = twilio_service._sid(read_write_user.email)

    data = PasswordWillResetSchema(
        code=TestConstants.CODE,
        password=TestConstants.NEW_PASSWORD,
        sid=sid,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/auth/reset',
            json=data.dict(),
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    user = User.find_by_email(
        read_write_user.email,
        test_client,
    )

    expected = True
    actual = auth_utility.authenticate_password(
        TestConstants.NEW_PASSWORD,
        user.password,
    )
    
    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.auth
async def test_will_reset_password_returns_400_when_sid_is_invalid(mocker, auth_service, auth_utility, read_write_user, ssm_service, twilio_service, test_client):
    from server.dependencies import (
        auth_service,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client

    mocker.patch(
        'server.services.twilio_service.TwilioService.authenticate_email',
        return_value=False,
    )
    
    sid = twilio_service.wants_reset_password(TestConstants.ALTERNATIVE_EMAIL)

    data = PasswordWillResetSchema(
        code=TestConstants.CODE,
        email=read_write_user.email,
        password=TestConstants.NEW_PASSWORD,
        sid='',
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/auth/reset',
            json=data.dict(),
        )

    expected = 400
    actual = response.status_code

    assert expected == actual

    user = User.find_by_email(
        read_write_user.email,
        test_client,
    )

    expected = True
    actual = auth_utility.authenticate_password(
        TestConstants.PASSWORD,
        user.password,
    )
    
    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.auth
async def test_sign_in_returns_200_and_uses_tfa_when_signin_credentials_are_valid(auth_service, read_write_user, test_client):
    from server.dependencies import (
        auth_service,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client

    data = UserLoginSchema(
        email=read_write_user.email,
        password=TestConstants.PASSWORD,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/auth/signin',
            json=data.dict(),
        )

    expected = 200
    actual = response.status_code

    assert expected == actual
    

@pytest.mark.asyncio
@pytest.mark.auth
async def test_sign_in_returns_400_and_when_signin_credentials_are_invalid(auth_service, read_write_user, test_client):
    from server.dependencies import (
        auth_service,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client

    data = UserLoginSchema(
        email=read_write_user.email,
        password=TestConstants.PASSWORD_FAKE,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/auth/signin',
            json=data.dict(),
        )

    expected = 400
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.auth
async def test_sign_in_returns_400_and_when_user_is_inactive(auth_service, read_write_user, test_client):
    from server.dependencies import (
        auth_service,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client

    with test_client.start_session() as session:
        collection = test_client.visibly.users
        collection.update_one(
            { 'email': read_write_user.email },
            { '$set': { 'status': UserStatusType.DISABLED } },
        )
    
    data = UserLoginSchema(
        email=read_write_user.email,
        password=TestConstants.PASSWORD,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/auth/signin',
            json=data.dict(),
        )

    expected = 400
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.auth
async def test_sign_in_returns_400_and_when_user_is_pending(auth_service, read_write_user, test_client):
    from server.dependencies import (
        auth_service,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client

    with test_client.start_session() as session:
        collection = test_client.visibly.users
        collection.update_one(
            { 'email': read_write_user.email },
            { '$set': { 'status': UserStatusType.PENDING } },
        )
    
    data = UserLoginSchema(
        email=read_write_user.email,
        password=TestConstants.PASSWORD,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/auth/signin',
            json=data.dict(),
        )

    expected = 400
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.auth
async def test_confirm_returns_200_and_jwt_when_tfa_credentials_are_valid(mocker, auth_service, read_write_user, ssm_service, twilio_service, test_client):
    from server.dependencies import (
        auth_service,
        docdb,
        ssm_service,
    )
    from server.main import app

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[ssm_service] = ssm_service

    mocker.patch(
        'server.services.twilio_service.TwilioService.authenticate_email',
        return_value=True,
    )
    
    sid = twilio_service._sid(read_write_user.email)

    data = User2FASchema(
        code=TestConstants.CODE,
        sid=sid,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/auth/confirm',
            json=data.dict(),
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    actual = response.json().get('access_token', None)

    assert actual is not None

    actual = response.json().get('expires_in', None)

    assert actual is not None

    minimum_length = len('refresh_token=""')
    pattern = re.compile('refresh_token=\w+')
    actual = len(pattern.search(response.headers['set-cookie'])[0])

    assert actual > minimum_length


@pytest.mark.asyncio
@pytest.mark.auth
async def test_confirm_returns_400_and_jwt_when_tfa_credentials_are_invalid(mocker, auth_service, read_write_user, ssm_service, twilio_service, test_client):
    from server.dependencies import (
        auth_service,
        docdb,
        ssm_service,
    )
    from server.main import app
    from server.services.twilio_service import TwilioService

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[ssm_service] = ssm_service

    mocker.patch(
        'server.services.twilio_service.TwilioService.authenticate_email',
        return_value=False,
    )
    
    sid = twilio_service._sid(read_write_user.email)

    data = User2FASchema(
        code=TestConstants.CODE,
        sid=sid,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/auth/confirm',
            json=data.dict(),
        )

    expected = 401
    actual = response.status_code

    assert expected == actual

    actual = response.json().get('access_token', None)

    assert actual is None

    actual = response.json().get('expires_in', None)

    assert actual is None

    actual = response.headers.get('set-cookie', None)

    assert actual is None


@pytest.mark.asyncio
@pytest.mark.auth
async def test_token_returns_200_when_refresh_token_is_valid(auth_service, read_write_user, read_write_user_access_token, read_write_user_refresh_token, test_client):
    from server.dependencies import (
        auth_service,
        bearer,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/auth/token',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
                'Cookie': f'refresh_token={read_write_user_refresh_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.auth
async def test_token_returns_400_when_user_is_inactive(auth_service, read_write_user, read_write_user_access_token, read_write_user_refresh_token, test_client):
    from server.dependencies import (
        auth_service,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    with test_client.start_session() as session:
        collection = test_client.visibly.users
        collection.update_one(
            { 'email': read_write_user.email },
            { '$set': { 'status': UserStatusType.DISABLED } },
        )
    
    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/auth/token',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
                'Cookie': f'refresh_token={read_write_user_refresh_token}',
            },
        )

    expected = 401
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.auth
async def test_token_returns_401_when_refresh_token_cookie_is_missing(auth_service, read_write_user, read_write_user_access_token, test_client):
    from server.dependencies import (
        auth_service,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/auth/token',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
                'Cookie': '',
            },
        )

    expected = 401
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.auth
async def test_sign_out_clears_refresh_token_from_cookie(auth_service, read_write_user, read_write_user_refresh_token, test_client):
    from server.dependencies import (
        auth_service,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[auth_service] = auth_service
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/auth/logout',
            headers={
                'Cookie': f'refresh_token={read_write_user_refresh_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    expected = 'refresh_token=""'
    pattern = re.compile('refresh_token=""')
    actual = pattern.search(response.headers['set-cookie'])[0]

    assert expected == actual
