import datetime
import logging

from bson.objectid import ObjectId

import httpx
import pytest

from server.core.constants import Constants
from server.resources.models.insight import Insight
from server.resources.models.tag import Tag
from server.resources.schema.tag import (
    TagCreateSchema,
    TagUpdateSchema,
)

from tests.test_constants import TestConstants


log = logging.getLogger(TestConstants.SERVER_TESTS)
log.setLevel(
    logging.INFO,
)


@pytest.mark.asyncio
@pytest.mark.tags
async def test_index_returns_tags(read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    with test_client.start_session() as session:
        collection = test_client.visibly.tags
        tag = collection.insert_one(
            {
                'name': TestConstants.TAG_NAME,
                'brand_id': test_brand._id,
            }
        )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            '/api/v1/tags',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    data = response.json()

    expected = 200
    actual = response.status_code

    assert expected == actual

    expected = 1
    actual = len(data)

    assert expected == actual

    expected = 0
    actual = data[0].get('count')

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.tags
async def test_index_groups_returns_group_tags_only(read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    with test_client.start_session() as session:
        collection = test_client.visibly.tags
        collection.insert_one(
            {
                'name': f'{TestConstants.TAG_NAME} 1',
                'brand_id': test_brand._id,
            }
        )
        collection.insert_one(
            {
                'name': f'{TestConstants.TAG_NAME} 2',
                'prefix': 'product',
                'brand_id': test_brand._id,
            }
        )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            '/api/v1/tags',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    data = response.json()

    expected = 200
    actual = response.status_code

    assert expected == actual

    expected = 2
    actual = len(data)

    assert expected == actual

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            '/api/v1/tags/groups',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    data = response.json()

    expected = 200
    actual = response.status_code

    assert expected == actual

    expected = 1
    actual = len(data)

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.tags
async def test_new_returns_campaigns_and_orders(read_write_user, read_write_user_access_token, test_client):
    from server.dependencies import (
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            '/api/v1/tags/new',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    data = response.json()
    actual = data.get('campaigns')

    assert actual is not None

    actual = data.get('orders')

    assert actual is not None


@pytest.mark.asyncio
@pytest.mark.tags
async def test_create_creates_tag(read_write_user, read_write_user_access_token, test_client):
    from server.dependencies import (
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = TagCreateSchema(
        name=TestConstants.TAG_NAME,
        prefix=TestConstants.TAG_PREFIX,
        orders=[
            '7704517000701',
        ],
        campaigns=[
            {
                'ad_type': 'sb',
                'campaignId': '144144206170275821'
            }
        ],
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/tags',
            json=data.dict(),
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    tag = Tag.find_by_id(
        ObjectId(response.json().get('id')),
        test_client,
    )

    assert tag is not None

    expected = TestConstants.TAG_NAME
    actual = tag.name

    assert expected == actual

    expected = TestConstants.TAG_PREFIX
    actual = tag.prefix

    assert expected == actual

    expected = 1
    actual = len(tag.campaigns)

    assert expected == actual

    expected = 1
    actual = len(tag.orders)

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.tags
async def test_update_updates_tag(read_write_user, read_write_user_access_token, test_client):
    from server.dependencies import (
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = TagCreateSchema(
        name=TestConstants.TAG_NAME,
        prefix=TestConstants.TAG_PREFIX,
        orders=[
            '7704517000701',
        ],
        campaigns=[
            {
                'ad_type': 'sb',
                'campaignId': '144144206170275821'
            }
        ],
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/tags',
            json=data.dict(),
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    tag = Tag.find_by_id(
        ObjectId(response.json().get('id')),
        test_client,
    )

    data = TagUpdateSchema(
        id=str(tag._id),
        name=TestConstants.TAG_NAME,
        prefix=TestConstants.TAG_PREFIX,
        orders=[
            '2634704010901',
            '7704517000701',
        ],
        campaigns=[
            {
                'ad_type': 'sb',
                'campaignId': '144144206170275821'
            },
            {
                'ad_type': 'sb',
                'campaignId': '144373579787038430'
            },
        ],
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            f'/api/v1/tags/{str(tag._id)}',
            json=data.dict(),
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    tag = Tag.find_by_id(
        ObjectId(response.json().get('id')),
        test_client,
    )

    expected = 2
    actual = len(tag.campaigns)

    assert expected == actual

    expected = 2
    actual = len(tag.orders)

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.tags
async def test_destroy_deletes_tag(read_write_user, read_write_user_access_token, test_client):
    from server.dependencies import (
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = TagCreateSchema(
        name=TestConstants.TAG_NAME,
        prefix=TestConstants.TAG_PREFIX,
        orders=[
            '7704517000701',
        ],
        campaigns=[
            {
                'ad_type': 'sb',
                'campaignId': '144144206170275821'
            }
        ],
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/tags',
            json=data.dict(),
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    tag = Tag.find_by_id(
        ObjectId(response.json().get('id')),
        test_client,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.delete(
            f'/api/v1/tags/{str(tag._id)}',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    tag = Tag.find_by_id(
        tag._id,
        test_client,
    )

    assert tag is None
    