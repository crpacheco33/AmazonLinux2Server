import logging

import httpx
import jwt
import pytest

from server.resources.models.brand import Brand
from server.resources.schema.brand import BrandIndexSchema
from tests.test_constants import TestConstants


log = logging.getLogger(TestConstants.SERVER_TESTS)
log.setLevel(
    logging.INFO,
)


@pytest.mark.asyncio
@pytest.mark.brands
async def test_index_returns_brands(auth_utility, read_write_user, read_write_user_access_token, test_client):
    from server.dependencies import (
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            '/api/v1/brands',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    expected = 2
    actual = len(response.json())

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.brands
async def test_index_returns_multiple_brands(auth_utility, shared_read_write_user, shared_read_write_user_access_token, test_client):
    from server.dependencies import (
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: shared_read_write_user

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            '/api/v1/brands',
            headers={
                'Authorization': f'Bearer {shared_read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    expected = 3
    actual = len(response.json())

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.brands
async def test_show_shows_a_brand(auth_utility, shared_read_write_user, shared_read_write_user_access_token, test_client):
    from server.dependencies import (
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: shared_read_write_user

    brand_id = str(Brand.find_by_name(f'{TestConstants.BRAND_NAME} 1', test_client)._id)

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            f'/api/v1/brands/{brand_id}',
            headers={
                'Authorization': f'Bearer {shared_read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.brands
async def test_update_updates_access_token(auth_utility, shared_read_write_user, shared_read_write_user_access_token, test_client):
    from server.dependencies import (
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: shared_read_write_user

    token = jwt.decode(shared_read_write_user_access_token, options={'verify_signature': False})
    
    expected = str(Brand.find_by_name(f'{TestConstants.BRAND_NAME} 1', test_client)._id)
    actual = token.get('brand')

    assert expected == actual

    brands = Brand.with_user(shared_read_write_user._id, test_client)
    brand_ids = [str(brand._id) for brand in brands]

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            f'/api/v1/brands/{brand_ids[2]}',
            headers={
                'Authorization': f'Bearer {shared_read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    encoded_token = response.json().get('access_token')
    token = jwt.decode(encoded_token, options={'verify_signature': False})

    expected = brand_ids[2]
    actual = token.get('brand')

    assert expected == actual