import logging
import uuid

from fastapi import (
    Request,
    Response,
)

import httpx
import pytest

from server.core.constants import Constants
from server.managers.brand_manager import BrandManager
from server.resources.schema.amazon_api import (
    UpdateSDAdGroupSchema,
    UpdateSDCampaignSchema,
    UpdateSDProductAdSchema,
    UpdateSDTargetSchema,
    UpdateSPAdGroupSchema,
    UpdateSPCampaignSchema,
    UpdateSPProductAdSchema,
    UpdateSPTargetSchema,
)
from server.utilities.aa_utility import AAUtility
from tests.test_constants import TestConstants
from tests.mocks.interface_mock import InterfaceMock


log = logging.getLogger(TestConstants.SERVER_TESTS)
log.setLevel(
    logging.INFO,
)


@pytest.mark.asyncio
@pytest.mark.amazon
async def test_index_sp_campaigns_returns_200_when_user_can_read_api(advertising_domain, read_write_user, read_write_user_access_token, sp_test_client, test_brand, test_client):
    from server.dependencies import (
        advertising_data,
        brand,
        docdb,
        Interface,
        ssm_service,
        user,
    )
    from server.main import app

    klass = AAUtility().campaign_interface_klass(
        Constants.SPONSORED_PRODUCTS,
    )
    interface = Interface(sp_test_client, klass)

    app.dependency_overrides[advertising_data] = lambda: advertising_domain
    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[ssm_service] = ssm_service
    app.dependency_overrides[user] = lambda: read_write_user

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            f'/api/v1/amazon/aa/sp/campaigns?startIndex=0&count=100',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.amazon
async def test_index_sp_campaigns_filters_campaigns_using_query(advertising_domain, read_write_user, read_write_user_access_token, sp_test_client, sandbox_brand, test_client):
    from server.dependencies import (
        advertising_data,
        brand,
        docdb,
        Interface,
        ssm_service,
        user,
    )
    from server.main import app

    klass = AAUtility().campaign_interface_klass(
        Constants.SPONSORED_PRODUCTS,
    )
    interface = Interface(sp_test_client, klass)

    app.dependency_overrides[advertising_data] = lambda: advertising_domain
    app.dependency_overrides[brand] = lambda: sandbox_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[ssm_service] = ssm_service
    app.dependency_overrides[user] = lambda: read_write_user

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            f'/api/v1/amazon/aa/sp/campaigns?startIndex=0&count=100',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    actual = response.json().get('totalCount')
    assert actual > 0

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            f'/api/v1/amazon/aa/sp/campaigns?query=not_a_real_query&startIndex=0&count=100',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    actual = response.json().get('totalCount')
    assert actual == 0


@pytest.mark.asyncio
@pytest.mark.amazon
async def test_index_sp_campaigns_returns_403_when_user_cannot_read_api(advertising_domain, write_user, write_user_access_token, sp_test_client, test_brand, test_client):
    from server.dependencies import (
        advertising_data,
        brand,
        docdb,
        user,
    )
    from server.main import app

    klass = AAUtility().campaign_interface_klass(
        Constants.SPONSORED_PRODUCTS,
    )

    app.dependency_overrides[advertising_data] = lambda: advertising_domain
    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: write_user

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            f'/api/v1/amazon/aa/sp/campaigns?startIndex=0&count=100',
            headers={
                'Authorization': f'Bearer {write_user_access_token}',
            },
        )

    expected = 403
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.amazon
async def test_index_sp_campaigns_returns_401_when_user_cannot_access_account(advertising_domain, read_write_user_alternative, read_write_user_access_token_alternative, sp_test_client, ssm_service, test_client):
    from server.dependencies import (
        advertising_data,
        docdb,
        Interface,
        ssm_service,
        user,
    )
    from server.main import app

    klass = AAUtility().campaign_interface_klass(
        Constants.SPONSORED_PRODUCTS,
    )
    interface = Interface(sp_test_client, klass)

    app.dependency_overrides[advertising_data] = lambda: advertising_domain
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[Interface] = lambda: interface
    app.dependency_overrides[ssm_service] = ssm_service
    app.dependency_overrides[user] = lambda: read_write_user_alternative
    
    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            f'/api/v1/amazon/aa/sp/campaigns?startIndex=0&count=100',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token_alternative}',
            },
        )

    expected = 401
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.amazon
@pytest.mark.skip(reason='Flaky')
async def test_update_sd_ad_group_updates_ad_group(
    read_write_user_access_token,
    sandbox_brand,
    sd_test_client,
):
    brand_manager = BrandManager()
    brand_manager.brand = sandbox_brand

    from server.dependencies import (
        brand,
        Interface,
        sd_client,
    )
    from server.main import app

    klass = AAUtility().ad_group_interface_klass(
        Constants.SPONSORED_DISPLAY,
    )
    interface = Interface(sd_test_client, klass)

    app.dependency_overrides[brand] = lambda: sandbox_brand
    app.dependency_overrides[sd_client] = lambda: sd_test_client

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('test.getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sd/campaigns',
            'query_string': 'stateFilter=enabled,paused'.encode(Constants.UTF8),
            'type': 'http',
        },
    )
    
    campaigns = await interface.index(
        advertiser_id=sandbox_brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=Response(),
    )

    campaign_id = campaigns[0].get('campaignId')

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('test.getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sd/ad_groups',
            'query_string': f'campaignIdFilter={campaign_id}&stateFilter=enabled,paused'.encode(Constants.UTF8),
            'type': 'http',
        },
    )
    
    ad_groups = await interface.index(
        advertiser_id=sandbox_brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=Response(),
    )

    existing_ad_group = ad_groups[0]
    existing_ad_group_id = existing_ad_group.get('adGroupId')
    existing_campaign_id = existing_ad_group.get('campaignId')
    existing_ad_group_name = existing_ad_group.get('name')

    updated_ad_group_name = 'Updated SD Ad Group Name'
    data = UpdateSDAdGroupSchema(
        adGroupId=existing_ad_group_id,
        campaignId=existing_campaign_id,
        name=updated_ad_group_name,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/amazon/aa/sd/ad_groups',
            json=[data.dict()],
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    updated_ad_groups = response.json()

    expected = 1
    actual = len(updated_ad_groups)

    assert expected == actual

    updated_ad_group = updated_ad_groups[0]

    expected = existing_ad_group_id
    actual = updated_ad_group.get('adGroupId')

    assert expected == actual

    expected = existing_campaign_id
    actual = updated_ad_group.get('campaignId')

    assert expected == actual

    expected = updated_ad_group_name
    actual = updated_ad_group.get('name')

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.amazon
@pytest.mark.skip(reason='Flaky')
async def test_update_sd_campaign_updates_campaign(
    read_write_user_access_token,
    sandbox_brand,
    sd_test_client,
):
    brand_manager = BrandManager()
    brand_manager.brand = sandbox_brand

    from server.dependencies import (
        brand,
        Interface,
        sd_client,
    )
    from server.main import app

    klass = AAUtility().campaign_interface_klass(
        Constants.SPONSORED_DISPLAY,
    )
    interface = Interface(sd_test_client, klass)

    app.dependency_overrides[brand] = lambda: sandbox_brand
    app.dependency_overrides[sd_client] = lambda: sd_test_client

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('test.getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sd/campaigns',
            'query_string': 'stateFilter=enabled,paused'.encode(Constants.UTF8),
            'type': 'http',
        },
    )
    
    campaigns = await interface.index(
        advertiser_id=sandbox_brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=Response(),
    )

    existing_campaign = campaigns[0]
    existing_campaign_id = existing_campaign.get('campaignId')
    existing_campaign_name = existing_campaign.get('name')

    updated_campaign_name = 'Updated SD Campaign Name'
    data = UpdateSDCampaignSchema(
        campaignId=existing_campaign_id,
        name=updated_campaign_name,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            f'/api/v1/amazon/aa/sd/campaigns',
            json=[data.dict()],
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    updated_campaigns = response.json()

    expected = 1
    actual = len(updated_campaigns)

    assert expected == actual

    updated_campaign = updated_campaigns[0]

    expected = existing_campaign_id
    actual = updated_campaign.get('campaignId')

    assert expected == actual

    expected = updated_campaign_name
    actual = updated_campaign.get('name')

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.amazon
@pytest.mark.skip(reason='Flaky')
async def test_update_sd_product_ad_updates_product_ad(
    read_write_user_access_token,
    sandbox_brand,
    sd_test_client,
):
    brand_manager = BrandManager()
    brand_manager.brand = sandbox_brand

    from server.dependencies import (
        brand,
        Interface,
        sd_client,
    )
    from server.main import app

    klass = AAUtility().product_ad_interface_klass(
        Constants.SPONSORED_DISPLAY,
    )
    interface = Interface(sd_test_client, klass)

    app.dependency_overrides[brand] = lambda: sandbox_brand
    app.dependency_overrides[sd_client] = lambda: sd_test_client

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('test.getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sd/product_ads',
            'query_string': 'stateFilter=enabled,paused'.encode(Constants.UTF8),
            'type': 'http',
        },
    )
    
    ads = await interface.index(
        advertiser_id=sandbox_brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=Response(),
    )

    existing_ad = ads[0]
    existing_ad_group_id = existing_ad.get('adGroupId')
    existing_ad_id = existing_ad.get('adId')
    existing_campaign_id = existing_ad.get('campaignId')
    existing_ad_state = existing_ad.get('state')

    updated_ad_state = 'paused'
    if existing_ad_state == 'paused':
        updated_ad_state = 'enabled'
    
    data = UpdateSDProductAdSchema(
        adGroupId=existing_ad_group_id,
        adId=existing_ad_id,
        campaignId=existing_campaign_id,
        state=updated_ad_state,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/amazon/aa/sd/product_ads',
            json=[data.dict()],
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    updated_ads = response.json()

    expected = 1
    actual = len(updated_ads)

    assert expected == actual

    updated_ad = updated_ads[0]

    expected = existing_ad_group_id
    actual = updated_ad.get('adGroupId')

    assert expected == actual

    expected = existing_ad_id
    actual = updated_ad.get('adId')

    assert expected == actual

    expected = existing_campaign_id
    actual = updated_ad.get('campaignId')

    assert expected == actual

    expected = updated_ad_state
    actual = updated_ad.get('state')

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.amazon
@pytest.mark.skip(reason='Flaky')
async def test_update_sd_target_updates_target(
    read_write_user_access_token,
    sandbox_brand,
    sd_test_client,
):
    brand_manager = BrandManager()
    brand_manager.brand = sandbox_brand

    from server.dependencies import (
        brand,
        Interface,
        sd_client,
    )
    from server.main import app

    klass = AAUtility().target_interface_klass(
        Constants.SPONSORED_DISPLAY,
    )
    interface = Interface(sd_test_client, klass)

    app.dependency_overrides[brand] = lambda: sandbox_brand
    app.dependency_overrides[sd_client] = lambda: sd_test_client

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('test.getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sd/targets',
            'query_string': 'stateFilter=enabled,paused'.encode(Constants.UTF8),
            'type': 'http',
        },
    )
    
    targets = await interface.index(
        advertiser_id=sandbox_brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=Response(),
    )

    existing_target = targets[0]
    existing_ad_group_id = existing_target.get('adGroupId')
    existing_campaign_id = existing_target.get('campaignId')
    existing_target_id = existing_target.get('targetId')
    existing_target_state = existing_target.get('state')

    updated_target_state = 'paused'
    if existing_target_state == 'paused':
        updated_target_state = 'enabled'
    
    data = UpdateSDTargetSchema(
        adGroupId=existing_ad_group_id,
        campaignId=existing_campaign_id,
        targetId=existing_target_id,
        state=updated_target_state,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/amazon/aa/sd/targets',
            json=[data.dict()],
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    updated_targets = response.json()
    
    expected = 1
    actual = len(updated_targets)

    assert expected == actual

    updated_target = updated_targets[0]

    expected = existing_ad_group_id
    actual = updated_target.get('adGroupId')

    assert expected == actual

    expected = existing_target_id
    actual = updated_target.get('targetId')

    assert expected == actual

    expected = updated_target_state
    actual = updated_target.get('state')

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.amazon
@pytest.mark.skip(reason='Flaky')
async def test_update_sp_ad_group_updates_ad_group(
    read_write_user_access_token,
    sandbox_brand,
    sp_test_client,
):
    brand_manager = BrandManager()
    brand_manager.brand = sandbox_brand

    from server.dependencies import (
        brand,
        Interface,
        sp_client,
    )
    from server.main import app

    klass = AAUtility().ad_group_interface_klass(
        Constants.SPONSORED_PRODUCTS,
    )
    interface = Interface(sp_test_client, klass)

    app.dependency_overrides[brand] = lambda: sandbox_brand
    app.dependency_overrides[sp_client] = lambda: sp_test_client

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('test.getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sp/campaigns',
            'query_string': 'stateFilter=enabled,paused'.encode(Constants.UTF8),
            'type': 'http',
        },
    )
    
    campaigns = await interface.index(
        advertiser_id=sandbox_brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=Response(),
    )

    campaign_id = campaigns[0].get('campaignId')

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('test.getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sp/ad_groups',
            'query_string': f'campaignIdFilter={campaign_id}&stateFilter=enabled,paused'.encode(Constants.UTF8),
            'type': 'http',
        },
    )
    
    ad_groups = await interface.index(
        advertiser_id=sandbox_brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=Response(),
    )

    existing_ad_group = ad_groups[0]
    existing_ad_group_id = existing_ad_group.get('adGroupId')
    existing_campaign_id = existing_ad_group.get('campaignId')
    existing_ad_group_name = existing_ad_group.get('name')

    updated_ad_group_name = 'Updated SP Ad Group Name'
    data = UpdateSPAdGroupSchema(
        adGroupId=existing_ad_group_id,
        campaignId=existing_campaign_id,
        name=updated_ad_group_name,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/amazon/aa/sp/ad_groups',
            json=[data.dict()],
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    updated_ad_groups = response.json()

    expected = 1
    actual = len(updated_ad_groups)

    assert expected == actual

    updated_ad_group = updated_ad_groups[0]

    expected = existing_ad_group_id
    actual = updated_ad_group.get('adGroupId')

    assert expected == actual

    expected = existing_campaign_id
    actual = updated_ad_group.get('campaignId')

    assert expected == actual

    expected = updated_ad_group_name
    actual = updated_ad_group.get('name')

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.amazon
@pytest.mark.skip(reason='Flaky')
async def test_update_sp_campaign_updates_campaign(
    read_write_user_access_token,
    sandbox_brand,
    sp_test_client,
):
    brand_manager = BrandManager()
    brand_manager.brand = sandbox_brand

    from server.dependencies import (
        brand,
        Interface,
        sp_client,
    )
    from server.main import app

    klass = AAUtility().campaign_interface_klass(
        Constants.SPONSORED_PRODUCTS,
    )
    interface = Interface(sp_test_client, klass)

    app.dependency_overrides[brand] = lambda: sandbox_brand
    app.dependency_overrides[sp_client] = lambda: sp_test_client

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('test.getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sp/campaigns',
            'query_string': 'stateFilter=enabled,paused'.encode(Constants.UTF8),
            'type': 'http',
        },
    )
    
    campaigns = await interface.index(
        advertiser_id=sandbox_brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=Response(),
    )

    existing_campaign = campaigns[0]
    existing_campaign_id = existing_campaign.get('campaignId')
    existing_campaign_name = existing_campaign.get('name')

    updated_campaign_name = 'Updated SP Campaign Name'
    data = UpdateSPCampaignSchema(
        campaignId=existing_campaign_id,
        name=updated_campaign_name,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            f'/api/v1/amazon/aa/sp/campaigns',
            json=[data.dict()],
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    updated_campaigns = response.json()

    expected = 1
    actual = len(updated_campaigns)

    assert expected == actual

    updated_campaign = updated_campaigns[0]

    expected = existing_campaign_id
    actual = updated_campaign.get('campaignId')

    assert expected == actual

    expected = updated_campaign_name
    actual = updated_campaign.get('name')

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.amazon
@pytest.mark.skip(reason='Flaky')
async def test_update_sp_product_ad_updates_product_ad(
    read_write_user_access_token,
    sandbox_brand,
    sp_test_client,
):
    brand_manager = BrandManager()
    brand_manager.brand = sandbox_brand

    from server.dependencies import (
        brand,
        Interface,
        sp_client,
    )
    from server.main import app

    klass = AAUtility().product_ad_interface_klass(
        Constants.SPONSORED_PRODUCTS,
    )
    interface = Interface(sp_test_client, klass)

    app.dependency_overrides[brand] = lambda: sandbox_brand
    app.dependency_overrides[sp_client] = lambda: sp_test_client

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('test.getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sp/product_ads',
            'query_string': 'stateFilter=enabled,paused'.encode(Constants.UTF8),
            'type': 'http',
        },
    )
    
    ads = await interface.index(
        advertiser_id=sandbox_brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=Response(),
    )

    existing_ad = ads[0]
    existing_ad_id = existing_ad.get('adId')
    existing_ad_state = existing_ad.get('state')

    updated_ad_state = 'paused'
    if existing_ad_state == 'paused':
        updated_ad_state = 'enabled'
    
    data = UpdateSPProductAdSchema(
        adId=existing_ad_id,
        state=updated_ad_state,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/amazon/aa/sp/product_ads',
            json=[data.dict()],
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    updated_ads = response.json()

    expected = 1
    actual = len(updated_ads)

    assert expected == actual

    updated_ad = updated_ads[0]

    expected = existing_ad_id
    actual = updated_ad.get('adId')

    assert expected == actual

    expected = updated_ad_state
    actual = updated_ad.get('state')

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.amazon
@pytest.mark.skip(reason='Flaky')
async def test_update_sp_target_updates_target(
    read_write_user_access_token,
    sandbox_brand,
    sp_test_client,
):
    brand_manager = BrandManager()
    brand_manager.brand = sandbox_brand

    from server.dependencies import (
        brand,
        Interface,
        sp_client,
    )
    from server.main import app

    klass = AAUtility().target_interface_klass(
        Constants.SPONSORED_PRODUCTS,
    )
    interface = Interface(sp_test_client, klass)

    app.dependency_overrides[brand] = lambda: sandbox_brand
    app.dependency_overrides[sp_client] = lambda: sp_test_client

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('test.getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sp/campaigns',
            'query_string': 'stateFilter=enabled,paused'.encode(Constants.UTF8),
            'type': 'http',
        },
    )
    
    campaigns = await interface.index(
        advertiser_id=sandbox_brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=Response(),
    )

    campaign_id = campaigns[0].get('campaignId')

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('test.getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sp/targets',
            'query_string': f'campaignIdFilter={campaign_id}&stateFilter=enabled,paused'.encode(Constants.UTF8),
            'type': 'http',
        },
    )
    
    targets = await interface.index(
        advertiser_id=sandbox_brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=Response(),
    )

    existing_target = targets[0]
    existing_target_id = existing_target.get('targetId')
    existing_target_state = existing_target.get('state')

    updated_target_state = 'paused'
    if existing_target_state == 'paused':
        updated_target_state = 'enabled'
    
    data = UpdateSPTargetSchema(
        targetId=existing_target_id,
        state=updated_target_state,
    )

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.put(
            '/api/v1/amazon/aa/sp/targets',
            json=[data.dict()],
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    updated_targets = response.json()
    
    expected = 1
    actual = len(updated_targets)

    assert expected == actual

    updated_target = updated_targets[0]

    expected = existing_target_id
    actual = updated_target.get('targetId')

    assert expected == actual

    expected = updated_target_state
    actual = updated_target.get('state')

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.amazon
@pytest.mark.skip(reason='Flaky')
async def test_sd_create_campaign_creates_campaign(
    advertiser,
    sandbox_brand,
    sd_ad_group_interface,
    sd_campaign_interface,
    sd_product_ad_interface,
    sd_target_interface,
):
    from server.dependencies import (
        brand,
    )
    from server.main import app

    app.dependency_overrides[brand] = sandbox_brand

    advertiser_id = sandbox_brand.amazon.aa.sa.advertiser_id
    identifier = uuid.uuid4()
    response = await sd_campaign_interface.create(
        advertiser_id=advertiser_id,
        data=[{
            'name': f'SD Campaign {uuid.uuid4()} [VY] ',
            'budget': '100.0',
            'budgetType': 'daily',
            'startDate': '20210901',
            'state': 'paused',
            'tactic': 'T00020',
        }],
        request=Request(scope={
            'query_string': None,
            'type': 'http',
        }),
    )
    
    expected = TestConstants.SUCCESS
    actual = response[0].get('code')

    assert expected == actual
    
    campaign_id = response[0].get('campaignId')

    response = await sd_ad_group_interface.create(
        advertiser_id=advertiser_id,
        data=[{
            'name': f'SD Ad Group {identifier} [VY]',
            'defaultBid': 10.0,
            'bidOptimization': 'clicks',
            'tactic': 'T00020',
            'state': 'enabled',
            'campaignId': campaign_id,
        }],
        request=Request(scope={
            'query_string': None,
            'type': 'http',
        }),
    )

    expected = TestConstants.SUCCESS
    actual = response[0].get('code')

    assert expected == actual
    
    ad_group_id = response[0].get('adGroupId')

    response = await sd_product_ad_interface.create(
        advertiser_id=advertiser_id,
        data=[{
            'asin': 'B07NFSZBTB',
            'state': 'enabled',
            'adGroupId': ad_group_id,
            'campaignId': campaign_id,
        }],
        request=Request(scope={
            'query_string': None,
            'type': 'http',
        }),
    )

    expected = TestConstants.SUCCESS
    actual = response[0].get('code')

    assert expected == actual
    
    product_ad_id = response[0].get('adId')

    response = await sd_target_interface.create(
        advertiser_id=advertiser_id,
        data=[{
            'bid': 1.0,
            'expressionType': 'manual',
            'expression': [
                { 'value': 'B07NFSZBTB', 'type': 'asinSameAs', },
            ],
            'state': 'enabled',
            'adGroupId': ad_group_id,
        }],
        request=Request(scope={
            'query_string': None,
            'type': 'http',
        }),
    )

    expected = TestConstants.SUCCESS
    actual = response[0].get('code')

    assert expected == actual

@pytest.mark.asyncio
@pytest.mark.amazon
@pytest.mark.skip(reason='Flaky')
async def test_sp_interface_creates_sp_models(
    advertiser,
    sandbox_brand,
    sp_ad_group_interface,
    sp_campaign_interface,
    sp_keyword_interface,
    sp_product_ad_interface,
    sp_target_interface,
):
    from server.dependencies import (
        brand,
    )
    from server.main import app

    app.dependency_overrides[brand] = sandbox_brand

    advertiser_id = sandbox_brand.amazon.aa.sa.advertiser_id
    identifier = uuid.uuid4()
    response = await sp_campaign_interface.create(
        advertiser_id=advertiser_id,
        data=[{
            'name': f'SP Campaign {identifier} [VY] ',
            'campaignType': 'sponsoredProducts',
            'dailyBudget': '100.0',
            'premiumBidAdjustment': True,
            'startDate': '20210901',
            'state': 'enabled',
            'targetingType': 'manual',
        }],
        request=Request(scope={
            'query_string': None,
            'type': 'http',
        }),
    )

    expected = TestConstants.SUCCESS
    actual = response[0].get('code')

    assert expected == actual
    
    campaign_id = response[0].get('campaignId')

    response = await sp_ad_group_interface.create(
        advertiser_id=advertiser_id,
        data=[{
            'name': f'SP Ad Group {identifier} 1 [VY]',
            'defaultBid': 10.0,
            'state': 'enabled',
            'campaignId': campaign_id,
        }],
        request=Request(scope={
            'query_string': None,
            'type': 'http',
        }),
    )

    response = await sp_ad_group_interface.create(
        advertiser_id=advertiser_id,
        data=[{
            'name': f'SP Ad Group {identifier} 2 [VY]',
            'defaultBid': 10.0,
            'state': 'enabled',
            'campaignId': campaign_id,
        }],
        request=Request(scope={
            'query_string': None,
            'type': 'http',
        }),
    )

    first_ad_group_id = response[0].get('adGroupId')

    response = await sp_ad_group_interface.create(
        advertiser_id=advertiser_id,
        data=[{
            'name': f'SP Ad Group {identifier} 3 [VY]',
            'defaultBid': 10.0,
            'state': 'enabled',
            'campaignId': campaign_id,
        }],
        request=Request(scope={
            'query_string': None,
            'type': 'http',
        }),
    )

    expected = TestConstants.SUCCESS
    actual = response[0].get('code')

    assert expected == actual

    second_ad_group_id = response[0].get('adGroupId')
    
    ad_group_id = response[0].get('adGroupId')

    response = await sp_product_ad_interface.create(
        advertiser_id=advertiser_id,
        data=[{
            'asin': 'B07NFSZBTB',
            'state': 'enabled',
            'adGroupId': ad_group_id,
            'campaignId': campaign_id,
        }],
        request=Request(scope={
            'query_string': None,
            'type': 'http',
        }),
    )

    expected = TestConstants.SUCCESS
    actual = response[0].get('code')

    assert expected == actual
    
    product_ad_id = response[0].get('adId')

    response = await sp_target_interface.create(
        advertiser_id=advertiser_id,
        data=[{
            'bid': 10.0,
            'expressionType': 'manual',
            'expression': [
                { 'value': 'B07NFSZBTB', 'type': 'asinSameAs', },
            ],
            'resolvedExpression': [
                { 'value': 'B07NFSZBTB', 'type': 'asinSameAs', },
            ],
            'state': 'enabled',
            'adGroupId': first_ad_group_id,
            'campaignId': campaign_id,
        }],
        request=Request(scope={
            'query_string': None,
            'type': 'http',
        }),
    )

    response = await sp_keyword_interface.create(
        advertiser_id=advertiser_id,
        data=[{
            'bid': 10.0,
            'keywordText': 'airpod',
            'matchType': 'exact',
            'state': 'enabled',
            'adGroupId': second_ad_group_id,
            'campaignId': campaign_id,
        }],
        request=Request(scope={
            'query_string': None,
            'type': 'http',
        }),
    )

    expected = TestConstants.SUCCESS
    actual = response[0].get('code')

    assert expected == actual
