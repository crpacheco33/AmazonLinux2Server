import logging

import httpx
import pytest

from server.core.constants import Constants
from server.resources.models.daypart import Daypart
from server.resources.schema.daypart import CreateUpdateDaypartSchema
from server.resources.types.data_types import (
    AdType,
    PlatformType,
)
from tests.test_constants import TestConstants


log = logging.getLogger(TestConstants.SERVER_TESTS)
log.setLevel(
    logging.INFO,
)


@pytest.mark.asyncio
@pytest.mark.dayparts
async def test_create_with_schedule_creates_daypart(read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client

    existing_daypart = Daypart.find(
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        campaign_id=TestConstants.CAMPAIGN_ID,
        platform=PlatformType.AA,
        region=test_brand.amazon.aa.region,
        client=test_client,
    )

    assert existing_daypart is None

    data = CreateUpdateDaypartSchema(
        ad_type=AdType.SP,
        campaign_id=TestConstants.CAMPAIGN_ID,
        platform=PlatformType.AA,
        schedule=TestConstants.DAYPART_PAUSED_SCHEDULE,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/daypart',
            json=[data.dict()],
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    existing_daypart = Daypart.find(
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        campaign_id=TestConstants.CAMPAIGN_ID,
        platform=PlatformType.AA,
        region=test_brand.amazon.aa.region,
        client=test_client,
    )

    assert existing_daypart is not None


@pytest.mark.asyncio
@pytest.mark.dayparts
async def test_create_dayparts_with_bids_creates_daypart(read_write_user_access_token, ssm_service, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client

    existing_daypart = Daypart.find(
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        campaign_id=TestConstants.CAMPAIGN_ID,
        platform=PlatformType.AA,
        region=test_brand.amazon.aa.region,
        client=test_client,
    )

    assert existing_daypart is None

    bids = [
        {
            'dynamic_type': 'amount',
            'action': 'increase',
            'bid_type': 'dynamic',
            'limit': 100.0,
            'value': '5',
        } for _ in range(168)
    ]
    empty_bids = [{} for _ in range(168)]

    bids[0::2] = empty_bids[0::2]
    
    data = {
        'ad_type': AdType.SP,
        'advertiser_id': test_brand.amazon.aa.sa.advertiser_id,
        'bids': bids,
        'campaign_id': TestConstants.CAMPAIGN_ID,
        'platform': PlatformType.AA,
        'schedule': TestConstants.DAYPART_PAUSED_SCHEDULE,
    }

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/daypart',
            json=[data],
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    existing_daypart = Daypart.find(
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        campaign_id=TestConstants.CAMPAIGN_ID,
        platform=PlatformType.AA,
        region=test_brand.amazon.aa.region,
        client=test_client,
    )

    assert existing_daypart is not None

    expected = bids
    actual = existing_daypart.bids

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.dayparts
async def test_update_dayparts_with_bids_returns_422_when_bids_are_invalid(read_write_user_access_token, ssm_service, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client

    bids = ['-25', '20.5', '21.5']

    data = CreateUpdateDaypartSchema.construct(
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        bids=bids,
        campaign_id=TestConstants.CAMPAIGN_ID,
        platform=PlatformType.AA,
        schedule=TestConstants.DAYPART_PAUSED_SCHEDULE,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/daypart',
            json=[data.dict()],
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 422
    actual = response.status_code

    assert expected == actual

    existing_daypart = Daypart.find(
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        campaign_id=TestConstants.CAMPAIGN_ID,
        platform=PlatformType.AA,
        region=test_brand.amazon.aa.region,
        client=test_client,
    )

    assert existing_daypart is None


@pytest.mark.asyncio
@pytest.mark.dayparts
async def test_update_dayparts_updates_dayparts_with_bids(read_write_user_access_token, ssm_service, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client

    data = CreateUpdateDaypartSchema(
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        campaign_id=TestConstants.CAMPAIGN_ID,
        platform=PlatformType.AA,
        schedule=TestConstants.DAYPART_PAUSED_SCHEDULE,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/daypart',
            json=[data.dict()],
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    bids = [
        {
            'dynamic_type': 'amount',
            'action': 'increase',
            'bid_type': 'dynamic',
            'limit': 100.0,
            'value': '5',
        } for _ in range(168)
    ]
    empty_bids = [{} for _ in range(168)]

    bids[0::2] = empty_bids[0::2]
    
    data = CreateUpdateDaypartSchema(
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        bids=bids,
        campaign_id=TestConstants.CAMPAIGN_ID,
        platform=PlatformType.AA,
        schedule=TestConstants.DAYPART_PAUSED_SCHEDULE,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/daypart',
            json=[data.dict()],
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    existing_daypart = Daypart.find(
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        campaign_id=TestConstants.CAMPAIGN_ID,
        platform=PlatformType.AA,
        region=test_brand.amazon.aa.region,
        client=test_client,
    )

    expected = bids
    actual = existing_daypart.bids

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.dayparts
async def test_update_dayparts_removes_daypart_when_schedule_is_absent(read_write_user_access_token, ssm_service, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client

    data = CreateUpdateDaypartSchema(
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        campaign_id=TestConstants.CAMPAIGN_ID,
        platform=PlatformType.AA,
        schedule=TestConstants.DAYPART_PAUSED_SCHEDULE,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/daypart',
            json=[data.dict()],
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    data = CreateUpdateDaypartSchema(
        ad_type=AdType.SP,
        campaign_id=TestConstants.CAMPAIGN_ID,
        platform=PlatformType.AA,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/daypart',
            json=[data.dict()],
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual

    existing_daypart = Daypart.find(
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        campaign_id=TestConstants.CAMPAIGN_ID,
        platform=PlatformType.AA,
        region=test_brand.amazon.aa.region,
        client=test_client,
    )

    assert existing_daypart is None


@pytest.mark.asyncio
@pytest.mark.dayparts
async def test_update_dayparts_returns_422_when_schedule_is_invalid(read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client

    data = CreateUpdateDaypartSchema.construct(
        ad_type=AdType.SP,
        advertiser_id=test_brand.amazon.aa.sa.advertiser_id,
        campaign_id=TestConstants.CAMPAIGN_ID,
        platform=PlatformType.AA,
        schedule=TestConstants.DAYPART_INVALID_SCHEDULE,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/daypart',
            json=[data.dict()],
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 422
    actual = response.status_code

    assert expected == actual
