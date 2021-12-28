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

import pymongo

from server.core.constants import Constants
from server.dependencies import (
    advertising_data,
    brand,
    Interface,
    read,
    sb_client,
    write,
)
from server.middleware.aa_middleware import AAMiddleware
from server.resources.schema.amazon_api import (
    APIIndexSchema,
    UpdateSBAdGroupSchema,
)
from server.services.aws_service import AWSService
from server.services.data_service import DataService
from server.utilities.aa_utility import AAUtility
from server.utilities.list_utility import partition_list


klass = AAUtility().ad_group_interface_klass(
    Constants.SPONSORED_BRANDS,
)
interface = Interface(sb_client, klass)
log = AWSService().log_service
router = APIRouter(
    prefix=Constants.AD_GROUPS_PREFIX,
    route_class=AAMiddleware,
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
    from_date: str = None,
    to_date: str = None,
    name: str = None,
    creativeType: str = None,
    adGroupIdFilter: str = None,
    campaignIdFilter: str = None,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
    source: DataService = Depends(
        advertising_data,
    ),
):
    log.info(
        f'Indexing SB ad groups...',
    )

    ad_groups = await interface.index(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=response,
    )
    total = len(ad_groups)
    
    ad_group_ids = [str(ad_group.get('adGroupId')) for ad_group in ad_groups]

    data = {}
    partitions = partition_list(ad_group_ids, Constants.ES_FILTER_ARRAY_LIMIT)

    for partition in partitions:
        es_data = source.sa_model(
            Constants.SPONSORED_BRANDS,
            'ad_group',
            partition,
            from_date,
            to_date,
        )
        data.update(es_data)

    response = []

    for ad_group in ad_groups:
        ad_group_id = str(ad_group.get('adGroupId'))
        if ad_group_id in data:
            ad_group.update(
                data.get(ad_group_id),
            )
        
        response.append(ad_group)
    
    log.info(
        f'Indexed SB ad groups',
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
    '/{ad_group_id}',
    dependencies=[
        Depends(read),
    ],
)
async def show(
    request: Request,
    response: Response,
    ad_group_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Showing SB ad group {ad_group_id}...',
    )

    response = await interface.show(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=ad_group_id,
        request=request,
        response=response,
    )
    
    log.info(
        f'Showed SB ad group {ad_group_id}',
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
    data: List[UpdateSBAdGroupSchema],
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Updating SB ad groups...',
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

    ad_groups = []
    ad_group_ids = [ad_group.get('adGroupId') for ad_group in response]

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sb/ad_groups',
            'query_string': b'',
            'type': 'http',
        }
    )

    for ad_group_id in ad_group_ids:
        # GETting the ad group without a Response object will update
        # the object's cache
        ad_group = await interface.show(
            advertiser_id=brand.amazon.aa.sa.advertiser_id,
            key=ad_group_id,
            request=request,
        )
        ad_groups.append(ad_group)

    log.info(
        f'Updated SB ad groups',
    )

    return ad_groups


@router.delete(
    '/{ad_group_id}',
    dependencies=[
        Depends(write),
    ],
)
async def destroy(
    request: Request,
    ad_group_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Archiving SB ad group {ad_group_id}...',
    )

    response = await interface.destroy(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=ad_group_id,
        request=request,
    )

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sb/ad_groups',
            'query_string': b'',
            'type': 'http',
        }
    )

    # GETting the ad group without a Response object will update
    # the object's cache
    ad_group = await interface.show(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=ad_group_id,
        request=request,
    )
    
    log.info(
        f'Archived SB ad group {ad_group_id}',
    )

    return ad_group
