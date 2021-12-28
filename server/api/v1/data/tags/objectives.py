from typing import Optional

import datetime

from bson.objectid import ObjectId
from fastapi import APIRouter
from fastapi.params import (
    Depends,
)

import pymongo

from server.core.constants import Constants
from server.dependencies import (
    advertising_data,
    docdb,
    read,
)
from server.resources.models.tag import Tag
from server.resources.types.data_types import (
    IntervalType,
    ObjectiveType,
    TableType,
)
from server.services.aws_service import AWSService
from server.services.data_service import DataService
from server.utilities.data_utility import DataUtility


data_utility = DataUtility()
log = AWSService().log_service
router = APIRouter(
    prefix=Constants.OBJECTIVES_PREFIX,
)


@router.get(
    '/dsp_and_sa/{tag_id}',
    dependencies=[
        Depends(read),
    ],
)
def dsp_and_sa_objectives(
    tag_id: str,
    from_date: datetime.date,
    to_date: datetime.date,
    interval: IntervalType,
    objectives: Optional[str] = None,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    source: DataService = Depends(
        advertising_data,
    ),
):
    log.info(
        f'Obtaining DSP and Sponsored Ads tag objectives...',
    )

    tag = Tag.find_by_id(
        ObjectId(tag_id),
        client,
    )

    campaign_ids = tag.campaigns
    order_ids = tag.orders

    objectives, segments = data_utility.to_objectives_and_segments(objectives)

    data = source.dsp_and_sa_tags(
        campaign_ids,
        order_ids,
        from_date,
        to_date,
        interval,
        objectives,
        segments,
    )

    log.info(
        f'Obtained DSP and Sponsored Ads tag objectives',
    )

    return data


@router.get(
    '/dsp/{tag_id}',
    dependencies=[
        Depends(read),
    ],
)
def dsp_objectives(
    tag_id: str,
    from_date: datetime.date,
    to_date: datetime.date,
    interval: IntervalType,
    objectives: Optional[str] = None,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    source: DataService = Depends(
        advertising_data,
    ),
):
    log.info(
        f'Obtaining DSP tag objectives...',
    )

    tag = Tag.find_by_id(
        ObjectId(tag_id),
        client,
    )

    order_ids = tag.orders

    objectives, segments = data_utility.to_objectives_and_segments(objectives)

    data = source.dsp_tags(
        order_ids,
        from_date,
        to_date,
        interval,
        objectives,
        segments,
    )

    log.info(
        f'Obtained DSP tag objectives',
    )

    return data


@router.get(
    '/sa/{tag_id}',
    dependencies=[
        Depends(read),
    ],
)
def sa_objectives(
    tag_id: str,
    from_date: datetime.date,
    to_date: datetime.date,
    interval: IntervalType,
    objectives: Optional[str] = None,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    source: DataService = Depends(
        advertising_data,
    ),
):
    log.info(
        f'Obtaining SA tag objectives...',
    )

    tag = Tag.find_by_id(
        ObjectId(tag_id),
        client,
    )

    campaign_ids = tag.campaigns

    objectives, segments = data_utility.to_objectives_and_segments(objectives)

    data = source.sa_tags(
        campaign_ids,
        from_date,
        to_date,
        interval,
        objectives,
    )

    log.info(
        f'Obtained SA tag objectives',
    )

    return data


@router.get(
    '/data/{tag_id}',
    dependencies=[
        Depends(read),
    ],
)
def data(
    tag_id: str,
    from_date: datetime.date,
    to_date: datetime.date,
    objectives: Optional[str] = None,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    source: DataService = Depends(
        advertising_data,
    ),
):
    log.info(
        f'Obtaining DSP and SA tag data...',
    )

    tag = Tag.find_by_id(
        ObjectId(tag_id),
        client,
    )

    campaign_ids = tag.campaigns
    order_ids = tag.orders
    
    objectives, segments = data_utility.to_objectives_and_segments(objectives)

    data = source.tag_statistics(
        campaign_ids,
        order_ids,
        from_date,
        to_date,
        objectives,
        segments,
    )

    log.info(
        f'Obtained DSP and SA tag data',
    )

    return data
