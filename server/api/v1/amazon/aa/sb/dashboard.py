from collections import defaultdict
from typing import Optional

import re

from fastapi import APIRouter
from fastapi.params import (
    Depends,
)

import pymongo

from server.core.constants import Constants
from server.dependencies import (
    advertising_data,
    brand,
    docdb,
    read,
)
from server.resources.models.brand import Brand
from server.resources.types.data_types import (
    IntervalType,
    ObjectiveType,
)
from server.services.aws_service import AWSService
from server.services.data_service import DataService
from server.utilities.data_utility import DataUtility


aws_service = AWSService()
data_utility = DataUtility()
log = aws_service.log_service
router = APIRouter(
    prefix=Constants.GRAPHS_PREFIX,
)


@router.get(
    Constants.INDEX_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
def index(
    from_date: str,
    to_date: str,
    interval: IntervalType,
    name: str = None,
    query: str = None,
    stateFilter: str = None,
    campaign_ids: Optional[str] = None,
    objectives: Optional[str] = None,
    brand: Brand = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    source: DataService = Depends(
        advertising_data,
    ),
):
    log.info(
        f'Querying SB dashboard data...',
    )

    if campaign_ids:
        campaign_ids = campaign_ids.split(
            Constants.COMMA,
        )
    elif name or query or stateFilter:
        database = client.amazon
        
        sb_query = defaultdict(list)
        
        sb_query['_path'] = {
            '$in': [
                '/api/v1/amazon/aa/sb/campaigns',
            ],
        }
        
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

        if name or query:  # query is sent as query or name
            or_query = sb_query.get('$or', [])
            
            if name:
                escaped_name = re.escape(name)
                name_pattern = re.compile(
                    escaped_name,
                    re.IGNORECASE,
                )

                or_query.extend([
                    { 'name': name_pattern, },
                ])

            if query:
                escaped_query = re.escape(query)
                query_pattern = re.compile(
                    escaped_query,
                    re.IGNORECASE,
                )
                or_query.extend([
                    { 'name': query_pattern, },
                ])

            sb_query['$or'] = or_query
                
        if stateFilter is not None:
            sb_query['state'] = {
                '$in': stateFilter.split(Constants.COMMA),
            }

        sb_campaigns = list(database[brand.amazon.aa.sa.advertiser_id].find(
            sb_query,
        ))
        
        campaign_ids = [int(campaign.get('campaignId')) for campaign in sb_campaigns]
    else:
        campaign_ids = []

    objectives, segments = data_utility.to_objectives_and_segments(objectives)

    response = source.sa_dashboard(
        campaign_ids,
        from_date,
        to_date,
        objectives,
        ad_type=Constants.SPONSORED_BRANDS,
        interval=interval,
    )

    log.info(
        f'Queried SB dashboard data',
    )

    return response
