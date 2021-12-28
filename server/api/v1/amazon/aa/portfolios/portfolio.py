from collections import defaultdict
from typing import (
    Any,
    List,
)

import json
import re

from fastapi import (
    APIRouter,
    Request,
    Response,
)
from fastapi.params import (
    Depends,
)

import pymongo

from server.core.constants import Constants
from server.delegates.aa_delegate import AADelegate
from server.dependencies import (
    advertising_data,
    brand,
    docdb,
    Interface,
    read,
    portfolios_client,
    write,
)
from server.middleware.aa_middleware import AAMiddleware
from server.resources.models.daypart import Daypart
from server.resources.schema.amazon_api import (
    APIIndexSchema,
    ShowPortfolioSchema,
    ShowSBCampaignSchema,
    ShowSDCampaignSchema,
    ShowSPCampaignSchema,
)

from server.services.aws_service import AWSService
from server.services.data_service import DataService
from server.utilities.aa_utility import AAUtility
from server.utilities.list_utility import partition_list


klass = AAUtility().portfolio_interface_klass()
interface = Interface(portfolios_client, klass)
aws_service = AWSService()
log = aws_service.log_service
router = APIRouter(
    prefix=Constants.PORTFOLIOS_PREFIX,
    route_class=AAMiddleware,
)


@router.get(
    Constants.LIST_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index(
    request: Request,
    response: Response,
    from_date: str = None,
    to_date: str = None,
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
        'Indexing portfolios...',
    )

    portfolios = await interface.index(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=response,
    )
    total = len(portfolios)
    
    database = client.amazon
    campaign_ids = []

    response = []
    for portfolio in portfolios:
        query = defaultdict(list)
        query['_path'] = {
            '$in': [
                '/api/v1/amazon/aa/sb/campaigns',
                '/api/v1/amazon/aa/sd/campaigns',
                '/api/v1/amazon/aa/sp/campaigns',
            ],
        }
        query['portfolioId'] = portfolio.get('portfolioId')

        if from_date and to_date:
            start_date_string = from_date
            end_date_string = to_date
            
            query['$and'] = [
                {
                    'startDate': { '$lte': end_date_string },
                },
                { 
                    '$or': [
                        {
                            'endDate': { '$exists': False },
                        },
                        {
                            '$and': [
                                {
                                    'endDate': { '$gte': start_date_string },
                                },
                            ],
                        },
                    ],
                },
            ]

        
        campaigns = database[brand.amazon.aa.sa.advertiser_id].find(
            query,
            { 'campaignId': 1, '_id': 0 },
        )
        
        campaign_ids = [int(campaign.get('campaignId')) for campaign in campaigns]
        
        data = {}
        partitions = partition_list(campaign_ids, Constants.ES_FILTER_ARRAY_LIMIT)

        for partition in partitions:
            es_data = source.sa_model(
                None,
                Constants.PORTFOLIO,
                partition,
                from_date,
                to_date,
            )
            data.update(es_data)

        portfolio_response = ShowPortfolioSchema(
            **portfolio,
        ).dict()
        
        portfolio_response.update(
            data.get('campaigns', {
                'total_clicks': 0,
                'total_impressions': 0,
                'total_sales': 0,
                'total_spend': 0,
                'total_units_sold': 0,
                'units_sold': 0,
                'ctr': 0,
            }),
        )
        response.append(portfolio_response)

    log.info(
        'Indexed portfolios',
    )

    return APIIndexSchema(
        data=response,
        total_count=total,
    )


@router.get(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index(
    request: Request,
    response: Response,
    name: str = None,
    query: str = None,
    portfolioIdFilter: str = None,
    campaignIdFilter: str = None,
    stateFilter: str = None,
    from_date: str = None,
    to_date: str = None,
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
        'Indexing portfolios...',
    )

    database = client.amazon
    portfolios = []

    if portfolioIdFilter:
        portfolios = portfolioIdFilter.split(Constants.COMMA)

    response, results = [], []
    sb_total, sp_total = 0, 0
    for portfolio in portfolios:
        sb_query = defaultdict(list)
        sd_query = defaultdict(list)
        sp_query = defaultdict(list)
        
        sb_query['_path'] = {
            '$in': [
                '/api/v1/amazon/aa/sb/campaigns',
            ],
        }
        sd_query['_path'] = {
            '$in': [
                '/api/v1/amazon/aa/sd/campaigns',
            ],
        }
        sp_query['_path'] = {
            '$in': [
                '/api/v1/amazon/aa/sp/campaigns',
            ],
        }
        
        sb_query['portfolioId'] = portfolio
        sd_query['portfolioId'] = portfolio
        sp_query['portfolioId'] = portfolio

        if from_date and to_date:
            start_date_string = from_date
            end_date_string = to_date
            
            sb_query['$and'] = [
                {
                    'startDate': { '$lte': end_date_string },
                },
                { 
                    '$or': [
                        {
                            'endDate': { '$exists': False },
                        },
                        {
                            '$and': [
                                {
                                    'endDate': { '$gte': start_date_string },
                                },
                            ],
                        },
                    ],
                },
            ]
            sd_query['$and'] = [
                {
                    'startDate': { '$lte': end_date_string },
                },
                { 
                    '$or': [
                        {
                            'endDate': { '$exists': False },
                        },
                        {
                            '$and': [
                                {
                                    'endDate': { '$gte': start_date_string },
                                },
                            ],
                        },
                    ],
                },
            ]
            sp_query['$and'] = [
                {
                    'startDate': { '$lte': end_date_string },
                },
                { 
                    '$or': [
                        {
                            'endDate': { '$exists': False },
                        },
                        {
                            '$and': [
                                {
                                    'endDate': { '$gte': start_date_string },
                                },
                            ],
                        },
                    ],
                },
            ]
        
        if name or query:  # query is sent as query or name
            sb_or_query = sb_query.get('$or', [])
            sd_or_query = sd_query.get('$or', [])
            sp_or_query = sp_query.get('$or', [])
            
            if name:
                escaped_name = re.escape(name)
                name_pattern = re.compile(
                    escaped_name,
                    re.IGNORECASE,
                )

                sb_or_query.extend([
                    { 'name': name_pattern, },
                ])
                sd_or_query.extend([
                    { 'name': name_pattern, },
                ])
                sp_or_query.extend([
                    { 'name': name_pattern, },
                ])

            if query:
                escaped_query = re.escape(query)
                query_pattern = re.compile(
                    escaped_query,
                    re.IGNORECASE,
                )
                sb_or_query.extend([
                    { 'name': query_pattern, },
                ])
                sd_or_query.extend([
                    { 'name': query_pattern, },
                ])
                sp_or_query.extend([
                    { 'name': query_pattern, },
                ])

            sb_query['$or'] = sb_or_query
            sd_query['$or'] = sd_or_query
            sp_query['$or'] = sp_or_query
            
        if stateFilter:
            sb_query['state'] = {
                '$in': stateFilter.split(Constants.COMMA),
            }
            sd_query['state'] = {
                '$in': stateFilter.split(Constants.COMMA),
            }
            sp_query['state'] = {
                '$in': stateFilter.split(Constants.COMMA),
            }

        sb_campaigns = list(database[brand.amazon.aa.sa.advertiser_id].find(
            sb_query,
        ))
        
        sd_campaigns = list(database[brand.amazon.aa.sa.advertiser_id].find(
            sd_query,
        ))
        
        sp_campaigns = list(database[brand.amazon.aa.sa.advertiser_id].find(
            sp_query,
        ))
        
        sb_campaign_ids = [int(campaign.get('campaignId')) for campaign in sb_campaigns]
        sb_total = len(sb_campaign_ids)
        sd_campaign_ids = [int(campaign.get('campaignId')) for campaign in sd_campaigns]
        sd_total = len(sd_campaign_ids)
        sp_campaign_ids = [int(campaign.get('campaignId')) for campaign in sp_campaigns]
        sp_total = len(sp_campaign_ids)

        data = {}
        partitions = partition_list(
            sb_campaign_ids + sd_campaign_ids + sp_campaign_ids,
            Constants.ES_FILTER_ARRAY_LIMIT,
        )

        for partition in partitions:
            es_data = source.sa_model(
                None,
                Constants.CAMPAIGN,
                partition,
                from_date,
                to_date,
            )
            data.update(es_data)

        sb_campaign_response = [
            ShowSBCampaignSchema(**campaign).dict() for campaign in sb_campaigns
        ]
        sd_campaign_response = [
            ShowSDCampaignSchema(**campaign).dict() for campaign in sd_campaigns
        ]
        sp_campaign_response = [
            ShowSPCampaignSchema(**campaign).dict() for campaign in sp_campaigns
        ]
        
        response = sb_campaign_response + sd_campaign_response + sp_campaign_response
        
        for campaign in response:
            campaign_id = campaign.get('campaign_id')
            if campaign_id in data:
                campaign.update(
                    data.get(campaign_id),
                )
            
            results.append(campaign)
        
    log.info(
        'Indexed portfolios',
    )

    return APIIndexSchema(
        data=results,
        total_count=sb_total + sd_total + sp_total,
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
    '/{portfolio_id}',
    dependencies=[
        Depends(read),
    ],
)
async def show(
    request: Request,
    response: Response,
    portfolio_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Showing portfolio {portfolio_id}...',
    )

    response = await interface.show(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=portfolio_id,
        request=request,
        response=response,
    )

    log.info(
        f'Showed portfolio {portfolio_id}',
    )

    if isinstance(response, list):
        return response[0]

    return response


@router.put(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(write),
    ],
)
async def update(
    request: Request,
    data: List[Any],
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Updating portfolios...',
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
        request=Request,
    )

    portfolios = []
    portfolio_ids = [portfolio.get('portfolioId') for portfolio in response]

    for path in [
        '/api/v1/amazon/aa/portfolios',
        '/api/v1/amazon/aa/portfolios/list',
        '/api/v1/amazon/aa/portfolios/graphs/index',
    ]:
        request = Request(
            scope={
                'headers': [],
                'method': 'GET',
                'scheme': 'http',
                'server': ('getvisibly.com', 443,),
                'path': path,
                'query_string': b'',
                'type': 'http',
            }
        )

        for portfolio_id in portfolio_ids:
            # GETting the campaign without a Response object will update
            # the object's cache
            portfolio = await interface.show(
                advertiser_id=brand.amazon.aa.sa.advertiser_id,
                key=portfolio_id,
                request=request,
            )
            portfolios.append(portfolio)

    log.info(
        f'Updated portfolios',
    )

    return portfolios


@router.delete(
    '/{portfolio_id}',
    dependencies=[
        Depends(write),
    ],
)
async def destroy(
    request: Request,
    portfolio_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Archiving portfolio {portfolio_id}...',
    )

    response = await interface.destroy(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=portfolio_id,
        request=request,
    )

    for path in [
        '/api/v1/amazon/aa/portfolios',
        '/api/v1/amazon/aa/portfolios/list',
        '/api/v1/amazon/aa/portfolios/graphs/index',
    ]:
        request = Request(
            scope={
                'headers': [],
                'method': 'GET',
                'scheme': 'http',
                'server': ('getvisibly.com', 443,),
                'path': path,
                'query_string': b'',
                'type': 'http',
            }
        )

        # GETting the campaign without a Response object will update
        # the object's cache
        portfolio = await interface.show(
            advertiser_id=brand.amazon.aa.sa.advertiser_id,
            key=portfolio_id,
            request=request,
        )

    log.info(
        f'Archived portfolio {portfolio_id}',
    )

    return response
