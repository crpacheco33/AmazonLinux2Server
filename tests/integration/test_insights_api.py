import datetime
import logging

from bson.objectid import ObjectId

import httpx
import pytest

from server.core.constants import Constants
from server.resources.models.insight import Insight
from server.resources.models.tag import Tag
from server.resources.schema.insight import (
    InsightCreateSchema,
    InsightMetaSchema,
    InsightUpdateSchema,
)
from server.resources.types.data_types import (
    AdType,
    InsightMetaStateType,
    InsightStateType,
    InsightType,
)
from tests.test_constants import TestConstants


log = logging.getLogger(TestConstants.SERVER_TESTS)
log.setLevel(
    logging.INFO,
)


@pytest.mark.asyncio
@pytest.mark.insights
async def test_index_returns_insights_in_reverse_chronological_order(read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = {
        'action': TestConstants.INSIGHT_ACTION,
        'date': TestConstants.INSIGHT_DATE,
        'description': TestConstants.INSIGHT_DESCRIPTION,
        'title': TestConstants.INSIGHT_TITLE,
        'tags': [],
    }

    later_data = {
        'action': TestConstants.INSIGHT_ACTION,
        'date': '01023001',
        'description': TestConstants.INSIGHT_DESCRIPTION,
        'title': TestConstants.INSIGHT_TITLE,
        'tags': [],
    }

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/insights',
            json=data,
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    earlier_insight_id = response.json().get('id')
    
    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/insights',
            json=later_data,
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )
    
    later_insight_id = response.json().get('id')

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            '/api/v1/insights',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )
    
    insights = response.json()
    
    expected = 2
    actual = len(insights)

    assert expected == actual

    expected = earlier_insight_id
    actual = insights[1].get('id')

    assert expected == actual
    
    expected = later_insight_id
    actual = insights[0].get('id')

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.insights
async def test_create_insight_without_tags_creates_insight(read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = {
        'action': TestConstants.INSIGHT_ACTION,
        'date': TestConstants.INSIGHT_DATE,
        'description': TestConstants.INSIGHT_DESCRIPTION,
        'title': TestConstants.INSIGHT_TITLE,
        'tags': [],
    }

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/insights',
            json=data,
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )
    
    with test_client.start_session() as session:
        collection = test_client.visibly.insights
        insight = collection.find_one(
            { 'title': TestConstants.INSIGHT_TITLE },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    expected = TestConstants.INSIGHT_ACTION
    actual = insight.get('action')

    assert expected == actual

    expected = TestConstants.INSIGHT_FORMATTED_DATE
    actual = insight.get('date')

    assert expected == actual

    expected = TestConstants.INSIGHT_DESCRIPTION
    actual = insight.get('description')

    assert expected == actual

    actual = insight.get('state')

    assert actual is None

    expected = TestConstants.INSIGHT_TITLE
    actual = insight.get('title')

    assert expected == actual

    expected = 0
    actual = len(insight.get('tags'))

    assert expected == actual

    expected = TestConstants.INSIGHT_MANUAL_TYPE
    actual = insight.get('type')

    assert expected == actual

    insight_id = insight.get('_id')

    assert insight_id is not None


@pytest.mark.asyncio
@pytest.mark.insights
async def test_create_insight_with_tags_creates_insight(read_write_user, read_write_user_access_token, test_brand, test_client):
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
        result = collection.insert_one(
            {
                'name': TestConstants.TAG_NAME,
                'brand_id': read_write_user.brands[0],
            }
        )

    data = {
        'action': TestConstants.INSIGHT_ACTION,
        'date': TestConstants.INSIGHT_DATE,
        'description': TestConstants.INSIGHT_DESCRIPTION,
        'title': TestConstants.INSIGHT_TITLE,
        'tags': [str(result.inserted_id)],
    }

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/insights',
            json=data,
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    with test_client.start_session() as session:
        collection = test_client.visibly.insights
        insight = collection.find_one(
            { 'title': TestConstants.INSIGHT_TITLE },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    actual_tags = insight.get('tags')

    expected = 1
    actual = len(actual_tags)

    assert expected == actual
    
    expected = result.inserted_id
    actual = actual_tags[0]

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.insights
async def test_show_returns_insight(read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = {
        'action': TestConstants.INSIGHT_ACTION,
        'date': TestConstants.INSIGHT_DATE,
        'description': TestConstants.INSIGHT_DESCRIPTION,
        'title': TestConstants.INSIGHT_TITLE,
        'tags': [],
    }

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/insights',
            json=data,
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    insight_id = response.json().get('id')

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            f'/api/v1/insights/{insight_id}',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    insight = response.json()

    assert insight is not None

    expected = insight_id
    actual = insight.get('id')
    
    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.insights
async def test_update_insight_updates_insight_with_meta(read_write_user, read_write_user_access_token, ssm_service, test_brand, test_client):
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
                'brand_id': read_write_user.brands[0],
            }
        )

    metadata = InsightMetaSchema(
        action=TestConstants.INSIGHT_META_ACTION,
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        campaign_id=TestConstants.CAMPAIGN_ID,
        insight_type=TestConstants.INSIGHT_META_TYPE,
        region=test_brand.amazon.aa.region,
        value=TestConstants.INSIGHT_META_VALUE,
    )

    with test_client.start_session(causal_consistency=True) as session:
        collection = test_client.visibly.insights
        insight = collection.insert_one(
            {
                'action': TestConstants.INSIGHT_ACTION,
                'date': datetime.datetime(3001, 1, 1, 0, 0, 0),
                'description': TestConstants.INSIGHT_DESCRIPTION,
                'meta': metadata.dict(),
                'title': TestConstants.INSIGHT_TITLE,
                'tags': [tag.inserted_id],
                'type': InsightType.AUTOMATED,
                'accounts': [str(read_write_user.brands[0])],
            }
        )

    updated_metadata = InsightMetaSchema(
        action=TestConstants.INSIGHT_META_ACTION,
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        campaign_id=TestConstants.CAMPAIGN_ID,
        insight_type=TestConstants.INSIGHT_META_TYPE,
        region=test_brand.amazon.aa.region,
        state=InsightMetaStateType.ACCEPTED,
        value=TestConstants.INSIGHT_META_VALUE,
    )
    data = {
        'action': TestConstants.INSIGHT_ACTION,
        'date': TestConstants.INSIGHT_DATE,
        'description': TestConstants.INSIGHT_DESCRIPTION,
        'meta': updated_metadata.dict(),
        'state': InsightStateType.FAVORITE,
        'title': TestConstants.INSIGHT_TITLE,
        'tags': [str(tag.inserted_id)],
        'type': InsightType.AUTOMATED,
        'brand_id': str(read_write_user.brands[0]),
    }

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            f'/api/v1/insights/{str(insight.inserted_id)}',
            json=data,
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    with test_client.start_session() as session:
        collection = test_client.visibly.insights
        insight = Insight.find_one(
            insight.inserted_id,
            test_client,
        )

    assert insight is not None
    
    expected = 200
    actual = response.status_code

    assert expected == actual

    actual_meta = insight.meta

    expected = InsightMetaStateType.ACCEPTED
    actual = actual_meta.get('state')

    assert expected == actual

    expected = InsightStateType.FAVORITE
    actual = insight.state

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.insights
async def test_update_insight_with_tags_replaces_existing_tags(read_write_user, read_write_user_access_token, test_brand, test_client):
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
                'brand_id': read_write_user.brands[0],
                'user_id': read_write_user._id,
            }
        )

    data = {
        'action': TestConstants.INSIGHT_ACTION,
        'date': TestConstants.INSIGHT_DATE,
        'description': TestConstants.INSIGHT_DESCRIPTION,
        'title': TestConstants.INSIGHT_TITLE,
        'tags': [str(tag.inserted_id)],
    }

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/insights',
            json=data,
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    insight = Insight.find_by_id(
        ObjectId(response.json().get('id')),
        test_client,
    )

    assert insight is not None

    with test_client.start_session() as session:
        collection = test_client.visibly.tags
        tag = Tag.find_by_id(
            tag.inserted_id,
            test_client,
        )

    data = {
        'action': TestConstants.INSIGHT_ACTION,
        'date': TestConstants.INSIGHT_DATE,
        'description': TestConstants.INSIGHT_DESCRIPTION,
        'title': TestConstants.INSIGHT_TITLE,
        'tags': [str(tag._id)],
    }

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            f'/api/v1/insights/{insight._id}',
            json=data,
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    insight = Insight.find_by_id(insight._id, test_client)

    assert insight is not None

    actual_tags = insight.tags

    expected = 1
    actual = len(actual_tags)

    assert expected == actual

    expected = str(tag._id)
    actual = actual_tags[0].get('id')

    assert expected == actual
    

@pytest.mark.asyncio
@pytest.mark.insights
async def test_destroy_insight_deletes_insight(read_write_user, read_write_user_access_token, test_brand, test_client):
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
                'brand_id': read_write_user.brands[0],
            }
        )

    data = {
        'action': TestConstants.INSIGHT_ACTION,
        'date': TestConstants.INSIGHT_DATE,
        'description': TestConstants.INSIGHT_DESCRIPTION,
        'title': TestConstants.INSIGHT_TITLE,
        'tags': [str(tag.inserted_id)],
    }

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/insights',
            json=data,
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    existing_insight = Insight.find_by_id(
        ObjectId(response.json().get('id')),
        test_client,
    )

    assert existing_insight is not None

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.delete(
            f'/api/v1/insights/{existing_insight._id}',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    existing_insight = Insight.find_by_id(
        existing_insight._id,
        test_client,
    )

    assert existing_insight is None


@pytest.mark.asyncio
@pytest.mark.insights
async def test_destroy_insight_does_not_delete_another_users_insight(read_write_user, read_write_user_access_token, read_write_user_same_account, read_write_user_access_token_same_account, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[user] = lambda: read_write_user
    app.dependency_overrides[docdb] = lambda: test_client

    with test_client.start_session() as session:
        collection = test_client.visibly.tags
        tag = collection.insert_one(
            {
                'name': TestConstants.TAG_NAME,
                'brand_id': read_write_user.brands[0],
            }
        )

    data = {
        'action': TestConstants.INSIGHT_ACTION,
        'date': TestConstants.INSIGHT_DATE,
        'description': TestConstants.INSIGHT_DESCRIPTION,
        'title': TestConstants.INSIGHT_TITLE,
        'tags': [str(tag.inserted_id)],
    }

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/insights',
            json=data,
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    existing_insight = Insight.find_by_id(
        ObjectId(response.json().get('id')),
        test_client,
    )

    assert existing_insight is not None

    app.dependency_overrides[user] = lambda: read_write_user_same_account

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.delete(
            f'/api/v1/insights/{existing_insight._id}',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token_same_account}',
            },
        )

    expected = 403
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.insights
async def test_accept_invokes_insight_action(mocker, read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    mocked_insight_service = mocker.patch('server.services.insight_service.InsightService.accept')

    metadata = InsightMetaSchema(
        action=TestConstants.INSIGHT_META_ACTION,
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        campaign_id=TestConstants.CAMPAIGN_ID,
        insight_type=TestConstants.INSIGHT_META_TYPE,
        region=test_brand.amazon.aa.region,
        value=TestConstants.INSIGHT_META_VALUE,
    )

    with test_client.start_session(causal_consistency=True) as session:
        collection = test_client.visibly.insights
        insight = collection.insert_one(
            {
                'action': TestConstants.INSIGHT_ACTION,
                'date': datetime.datetime(3001, 1, 1, 0, 0, 0),
                'description': TestConstants.INSIGHT_DESCRIPTION,
                'meta': metadata.dict(),
                'title': TestConstants.INSIGHT_TITLE,
                'tags': [],
                'type': InsightType.AUTOMATED,
                'accounts': [str(read_write_user.brands[0])],
            }
        )

    insight_id = str(insight.inserted_id)

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            f'/api/v1/insights/{insight_id}/accept',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    mocked_insight_service.assert_called_once()

    insight = response.json()

    expected = 'accepted'
    actual = insight.get('meta').get('state')

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.insights
async def test_dismiss_does_not_invoke_action(mocker, read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    mocked_insight_service = mocker.patch('server.services.insight_service.InsightService.dismiss')

    metadata = InsightMetaSchema(
        action=TestConstants.INSIGHT_META_ACTION,
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        campaign_id=TestConstants.CAMPAIGN_ID,
        insight_type=TestConstants.INSIGHT_META_TYPE,
        region=test_brand.amazon.aa.region,
        value=TestConstants.INSIGHT_META_VALUE,
    )

    with test_client.start_session(causal_consistency=True) as session:
        collection = test_client.visibly.insights
        insight = collection.insert_one(
            {
                'action': TestConstants.INSIGHT_ACTION,
                'date': datetime.datetime(3001, 1, 1, 0, 0, 0),
                'description': TestConstants.INSIGHT_DESCRIPTION,
                'meta': metadata.dict(),
                'title': TestConstants.INSIGHT_TITLE,
                'tags': [],
                'type': InsightType.AUTOMATED,
                'accounts': [str(read_write_user.brands[0])],
            }
        )

    insight_id = str(insight.inserted_id)

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            f'/api/v1/insights/{insight_id}/dismiss',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    mocked_insight_service.assert_called_once()

    insight = response.json()

    expected = 'dismissed'
    actual = insight.get('meta').get('state')

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.insights
async def test_dismiss_does_not_dismiss_accepted_insight(mocker, read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    mocked_insight_service = mocker.patch('server.services.insight_service.InsightService.accept')

    metadata = InsightMetaSchema(
        action=TestConstants.INSIGHT_META_ACTION,
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        campaign_id=TestConstants.CAMPAIGN_ID,
        insight_type=TestConstants.INSIGHT_META_TYPE,
        region=test_brand.amazon.aa.region,
        state=InsightMetaStateType.ACCEPTED,
        value=TestConstants.INSIGHT_META_VALUE,
    )

    with test_client.start_session(causal_consistency=True) as session:
        collection = test_client.visibly.insights
        insight = collection.insert_one(
            {
                'action': TestConstants.INSIGHT_ACTION,
                'date': datetime.datetime(3001, 1, 1, 0, 0, 0),
                'description': TestConstants.INSIGHT_DESCRIPTION,
                'meta': metadata.dict(),
                'title': TestConstants.INSIGHT_TITLE,
                'tags': [],
                'type': InsightType.AUTOMATED,
                'accounts': [str(read_write_user.brands[0])],
            }
        )

    insight_id = str(insight.inserted_id)

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            f'/api/v1/insights/{insight_id}/dismiss',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    mocked_insight_service.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.insights
async def test_accept_does_not_accept_manual_insight(mocker, read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    mocked_insight_service = mocker.patch('server.services.insight_service.InsightService.accept')

    with test_client.start_session(causal_consistency=True) as session:
        collection = test_client.visibly.insights
        insight = collection.insert_one(
            {
                'action': TestConstants.INSIGHT_ACTION,
                'date': datetime.datetime(3001, 1, 1, 0, 0, 0),
                'description': TestConstants.INSIGHT_DESCRIPTION,
                'title': TestConstants.INSIGHT_TITLE,
                'tags': [],
                'type': InsightType.MANUAL,
                'accounts': [str(read_write_user.brands[0])],
            }
        )

    insight_id = str(insight.inserted_id)

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            f'/api/v1/insights/{insight_id}/accept',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    mocked_insight_service.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.insights
async def test_dismiss_does_not_dismiss_manual_insight(mocker, read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    mocked_insight_service = mocker.patch('server.services.insight_service.InsightService.dismiss')

    with test_client.start_session(causal_consistency=True) as session:
        collection = test_client.visibly.insights
        insight = collection.insert_one(
            {
                'action': TestConstants.INSIGHT_ACTION,
                'date': datetime.datetime(3001, 1, 1, 0, 0, 0),
                'description': TestConstants.INSIGHT_DESCRIPTION,
                'title': TestConstants.INSIGHT_TITLE,
                'tags': [],
                'type': InsightType.MANUAL,
                'accounts': [str(read_write_user.brands[0])],
            }
        )

    insight_id = str(insight.inserted_id)

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            f'/api/v1/insights/{insight_id}/dismiss',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    mocked_insight_service.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.insights
async def test_dismiss_does_nothing_on_dismissed_insight(mocker, read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    mocked_insight_service = mocker.patch('server.services.insight_service.InsightService.dismiss')

    metadata = InsightMetaSchema(
        action=TestConstants.INSIGHT_META_ACTION,
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        campaign_id=TestConstants.CAMPAIGN_ID,
        insight_type=TestConstants.INSIGHT_META_TYPE,
        region=test_brand.amazon.aa.region,
        state=InsightMetaStateType.DISMISSED,
        value=TestConstants.INSIGHT_META_VALUE,
    )

    with test_client.start_session(causal_consistency=True) as session:
        collection = test_client.visibly.insights
        insight = collection.insert_one(
            {
                'action': TestConstants.INSIGHT_ACTION,
                'date': datetime.datetime(3001, 1, 1, 0, 0, 0),
                'description': TestConstants.INSIGHT_DESCRIPTION,
                'meta': metadata.dict(),
                'title': TestConstants.INSIGHT_TITLE,
                'tags': [],
                'type': InsightType.AUTOMATED,
                'accounts': [str(read_write_user.brands[0])],
            }
        )

    insight_id = str(insight.inserted_id)

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            f'/api/v1/insights/{insight_id}/dismiss',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    mocked_insight_service.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.insights
async def test_like_likes_insight(read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    with test_client.start_session(causal_consistency=True) as session:
        collection = test_client.visibly.insights
        insight = collection.insert_one(
            {
                'action': TestConstants.INSIGHT_ACTION,
                'date': datetime.datetime(3001, 1, 1, 0, 0, 0),
                'description': TestConstants.INSIGHT_DESCRIPTION,
                'title': TestConstants.INSIGHT_TITLE,
                'tags': [],
                'type': InsightType.MANUAL,
                'accounts': [str(read_write_user.brands[0])],
            }
        )

    insight_id = str(insight.inserted_id)

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            f'/api/v1/insights/{insight_id}/like',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    insight = response.json()

    expected = 'favorite'
    actual = insight.get('state')

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.insights
async def test_unlike_unlikes_insight(read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    with test_client.start_session(causal_consistency=True) as session:
        collection = test_client.visibly.insights
        insight = collection.insert_one(
            {
                'action': TestConstants.INSIGHT_ACTION,
                'date': datetime.datetime(3001, 1, 1, 0, 0, 0),
                'description': TestConstants.INSIGHT_DESCRIPTION,
                'title': TestConstants.INSIGHT_TITLE,
                'tags': [],
                'type': InsightType.MANUAL,
                'accounts': [str(read_write_user.brands[0])],
            }
        )

    insight_id = str(insight.inserted_id)

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            f'/api/v1/insights/{insight_id}/like',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    insight = response.json()

    expected = 'favorite'
    actual = insight.get('state')

    assert expected == actual

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            f'/api/v1/insights/{insight_id}/unlike',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    insight = response.json()

    actual = insight.get('state')

    assert actual is None
