from typing import (
    Any,
    List,
)

import json

from fastapi import (
    APIRouter,
    Request,
    Response,
)
from fastapi.params import (
    Depends,
)
from fastapi.responses import JSONResponse

import pymongo

from server.core.constants import Constants
from server.delegates.aa_delegate import AADelegate
from server.dependencies import (
    advertising_data,
    brand,
    docdb,
    Interface,
    read,
    sb_client,
    write,
)
from server.middleware.aa_middleware import AAMiddleware
from server.resources.models.daypart import Daypart
from server.resources.schema.amazon_api import (
    APIIndexSchema,
    UpdateSBCampaignSchema,
)

from server.resources.types.data_types import (
    AdType,
    PlatformType,
    RegionType,
)
from server.services.aws_service import AWSService
from server.services.data_service import DataService
from server.utilities.aa_utility import AAUtility
from server.utilities.list_utility import partition_list


aa_utility = AAUtility()
klass = aa_utility.campaign_budget_rule_interface_klass(
    Constants.SPONSORED_BRANDS,
)
campaign_budget_rule_interface = Interface(sb_client, klass)

klass = aa_utility.campaign_interface_klass(
    Constants.SPONSORED_BRANDS,
)
interface = Interface(sb_client, klass)
aws_service = AWSService()
log = aws_service.log_service
router = APIRouter(
    prefix=Constants.CAMPAIGNS_PREFIX,
    route_class=AAMiddleware,
)


@router.get(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(read),
    ],
    response_model=APIIndexSchema,
    response_model_by_alias=True,
)
async def index(
    request: Request,
    response: Response,
    query: str = None,
    from_date: str = None,
    to_date: str = None,
    name: str = None,
    adFormatFilter: str = None,
    creativeType: str = None,
    stateFilter: str = None,
    campaignIdFilter: str = None,
    portfolioIdFilter: str = None,
    brand: Any = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    interface: Interface = Depends(
        interface,
    ),
    source: DataService = Depends(
        advertising_data,
    ),
):
    log.info(
        f'Indexing SB campaigns...',
    )

    campaigns = await interface.index(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=response,
    )
    total = len(campaigns)

    campaign_ids = [str(campaign.get('campaignId')) for campaign in campaigns]

    data = {}
    partitions = partition_list(campaign_ids, Constants.ES_FILTER_ARRAY_LIMIT)

    for partition in partitions:
        es_data = source.sa_model(
            Constants.SPONSORED_BRANDS,
            Constants.CAMPAIGN,
            partition,
            from_date,
            to_date,
        )
        data.update(es_data)

    dayparts = Daypart.find_all(
        AdType.SB,
        brand.amazon.aa.sa.advertiser_id,
        campaign_ids,
        PlatformType.AA,
        brand.amazon.aa.region,
        client,
    )

    bids, schedules = {}, {}

    for daypart in dayparts:
        bids[daypart.campaign_id] = daypart.bids
        schedules[daypart.campaign_id] = daypart.schedule

    response = []

    for campaign in campaigns:
        campaign_id = str(campaign.get('campaignId'))
        if campaign_id in data:
            campaign.update(
                data.get(campaign_id)
            )

        if campaign_id in bids:
            campaign[Constants.BIDS] = bids.get(
                campaign_id,
            )

        if campaign_id in schedules:
            campaign[Constants.SCHEDULE] = schedules.get(
                campaign_id,
            )

        response.append(campaign)

    log.info(
        f'Indexed SB campaigns',
    )

    return APIIndexSchema(
        data=response,
        total_count=total,
    )


@router.post(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(write),
    ],
)
async def create(
    request: Request,
    data: List[Any],
):
    pass


@router.get(
    '/{campaign_id}',
    dependencies=[
        Depends(read),
    ],
)
async def show(
    request: Request,
    response: Response,
    campaign_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Showing SB campaign {campaign_id}...',
    )

    response = await interface.show(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=campaign_id,
        request=request,
        response=response,
    )

    if isinstance(response, list):
        response = response[0]

    log.info(
        f'Showed SB campaign {campaign_id}',
    )

    return response


@router.put(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(write),
    ],
)
async def update(
    request: Request,
    data: List[UpdateSBCampaignSchema],
    brand: Any = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Updating SB campaigns...',
    )

    data = [
        json.loads(
            datum.json(
                exclude_none=True,
                exclude_defaults=True,
            ),
        )
        for datum in data
    ]

    response = await interface.update(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        data=data,
        request=request,
    )

    campaigns = []
    campaign_ids = [campaign.get('campaignId') for campaign in response]

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sb/campaigns',
            'query_string': b'',
            'type': 'http',
        }
    )

    for campaign_id in campaign_ids:
        # GETting the campaign without a Response object will update
        # the object's cache
        campaign = await interface.show(
            advertiser_id=brand.amazon.aa.sa.advertiser_id,
            key=campaign_id,
            request=request,
        )
        campaigns.append(campaign)
    
    log.info(
        f'Updated SB campaigns',
    )

    return campaigns
    

@router.delete(
    '/{campaign_id}',
    dependencies=[
        Depends(write),
    ],
)
async def destroy(
    request: Request,
    campaign_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Archiving SB campaign {campaign_id}...',
    )

    response = await interface.destroy(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=campaign_id,
        request=request,
    )

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sb/campaigns',
            'query_string': b'',
            'type': 'http',
        }
    )

    # GETting the campaign without a Response object will update
    # the object's cache
    campaign = await interface.show(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=campaign_id,
    )

    log.info(
        f'Archived SB campaign {campaign_id}',
    )

    return campaign


@router.get(
    '/{campaign_id}/budget_rules',
    dependencies=[
        Depends(read),
    ],
)
async def index_budget_rules(
    request: Request,
    response: Response,
    campaign_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        campaign_budget_rule_interface,
    ),
):
    log.info(
        f'Indexing SB campaign {campaign_id} budget rules...',
    )

    response = await interface.index(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        request=request,
    )

    budget_rules = response.get(
        'associatedRules',
        [],
    )

    log.info(
        f'Indexed SB campaign {campaign_id} budget rules',
    )

    return budget_rules


@router.post(
    '/{campaign_id}/budget_rules',
    dependencies=[
        Depends(write),
    ],
)
async def create_budget_rules(
    request: Request,
    campaign_id: str,
    data: List[Any],
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        campaign_budget_rule_interface,
    ),
):
    log.info(
        f'Creating budget rules for campaign {campaign_id}...',
    )

    response = interface.create(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        data=data,
        request=request,
    )

    log.info(
        f'Created budget rules for campaign {campaign_id}',
    )

    return response


@router.get(
    '/{campaign_id}/budget_rules/budget_history',
    dependencies=[
        Depends(read),
    ],
)
async def index_budget_rules_history(
    request: Request,
    response: Response,
    campaign_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        campaign_budget_rule_interface,
    ),
):
    log.info(
        f'Indexing SB campaign {campaign_id} budget rule history...',
    )

    response = await campaign_budget_rule_interface.history(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=campaign_id,
        request=request,
        response=response,
    )

    log.info(
        f'Indexed SB campaign {campaign_id} budget rule history',
    )

    return response


@router.delete(
    '/{campaign_id}/budgetRules/{budget_rule_id}',
    dependencies=[
        Depends(write),
    ],
)
async def destroy_budget_rule(
    request: Request,
    campaign_id: str,
    budget_rule_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        campaign_budget_rule_interface,
    ),
):
    log.info(
        f'Disassociating SB budget rule {budget_rule_id} from {campaign_id}...',
    )

    response = await interface.destroy(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=campaign_id,
        request=request,
    )

    log.info(
        f'Disassociated SB budget rule {budget_rule_id} from {campaign_id}',
    )

    return campaign
