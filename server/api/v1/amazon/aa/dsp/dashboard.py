from collections import defaultdict
from datetime import datetime
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
    FunnelType,
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
    order_ids: Optional[str] = None,
    objectives: Optional[str] = None,
    segments: Optional[str] = None,
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
        f'Querying DSP dashboard data...',
    )

    if order_ids:
        order_ids = order_ids.split(
            Constants.COMMA,
        )
    elif name or query:
        database = client.amazon
        
        dsp_query = defaultdict(list)
        
        dsp_query['_path'] = {
            '$in': [
                '/api/v1/amazon/aa/dsp/orders',
            ],
        }
        
        if from_date and to_date:
            start_date = datetime.strptime(
                from_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )
            start_date_string = datetime.strftime(
                start_date,
                Constants.DATE_FORMAT_DSP,
            )
            end_date = datetime.strptime(
                to_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )
            end_date_string = datetime.strftime(
                end_date,
                Constants.DATE_FORMAT_DSP,
            )

            dsp_query['$and'] = [
                {
                    'startDateTime': { '$lte': end_date_string },
                },
                { 
                    '$or': [
                        {
                            'endDateTime': { '$exists': False },
                        },
                        {
                            '$and': [
                                {
                                    'endDateTime': { '$gte': start_date_string },
                                },
                            ],
                        },
                    ],
                },
            ]

        if name or query:
            or_query = dsp_query.get('$or', [])
            
            if name:
                escaped_name = re.escape(name)
                name_pattern = re.compile(
                    escaped_name,
                    re.IGNORECASE,
                )

                or_query.extend([
                    { 'orderName': { '$regex': name_pattern }, },
                ])

            if query:
                escaped_query = re.escape(query)
                query_pattern = re.compile(
                    escaped_query,
                    re.IGNORECASE,
                )
                or_query.extend([
                    { 'orderName': { '$regex': query_pattern }, },
                ])


            dsp_query['$or'] = or_query 

        dsp_orders = list(database[brand.amazon.aa.dsp.advertiser_id].find(
            dsp_query,
        ))
        
        order_ids = [order.get('orderId') for order in dsp_orders]
    else:
        order_ids = []

    objectives, segments = data_utility.to_objectives_and_segments(objectives)

    response = source.dsp_dashboard(
        order_ids,
        from_date,
        to_date,
        interval,
        segments,
        objectives,
    )

    log.info(
        f'Queried DSP dashboard data',
    )

    return response
