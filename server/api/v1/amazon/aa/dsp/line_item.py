import json

from typing import (
    Any,
    List,
)

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
    dsp_client,
    Interface,
    read,
    write,
)
from server.middleware.aa_middleware import AAMiddleware
from server.resources.schema.amazon_api import (
    APIIndexSchema,
)
from server.resources.schema.dsp_schema import (
    LineItem,
)
from server.services.aws_service import AWSService
from server.services.data_service import DataService
from server.utilities.aa_utility import AAUtility
from server.utilities.list_utility import partition_list


klass = AAUtility().line_item_interface_klass()
interface = Interface(dsp_client, klass)
log = AWSService().log_service
router = APIRouter(
    prefix=Constants.LINE_ITEMS_PREFIX,
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
    from_date: str = None,
    to_date: str = None,
    lineItemIdFilter: str = None,
    orderIdFilter: str = None,
    statusFilter: str = None,
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
        f'Indexing DSP line items...',
    )

    line_items = await interface.index(
        advertiser_id=brand.amazon.aa.dsp.advertiser_id,
        request=request,
        response=response,
    )
    total = len(line_items)

    line_item_ids = [line_item.get('lineItemId') for line_item in line_items]

    data = {}
    partitions = partition_list(line_item_ids, Constants.ES_FILTER_ARRAY_LIMIT)

    for partition in partitions:
        es_data = source.dsp_model(
            Constants.DSP,
            'line_item',
            partition,
            from_date,
            to_date,
        )
        data.update(es_data)

    response = []

    for line_item in line_items:
        line_item_id = line_item.get('lineItemId')
        if line_item_id in data:
            line_item.update(
                data.get(line_item_id)
            )

        response.append(line_item)
    
    log.info(
        f'Indexed DSP line items',
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
    '/{line_item_id}',
    dependencies=[
        Depends(read),
    ],
)
async def show(
    request: Request,
    response: Response,
    line_item_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Showing DSP line item {line_item_id}...',
    )

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/dsp/line_items',
            'query_string': b'',
            'type': 'http',
        }
    )

    response = await interface.show(
        advertiser_id=brand.amazon.aa.dsp.entity_id,
        key=line_item_id,
        request=request,
    )

    log.info(
        f'Showed DSP line item {line_item_id}',
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
    data: List[LineItem],
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Updating DSP line items...',
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
        advertiser_id=brand.amazon.aa.dsp.entity_id,
        data=data,
        request=request,
    )

    line_items = []
    line_item_ids = [line_item.get('lineItemId') for line_item in response]

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/dsp/line_items',
            'query_string': b'',
            'type': 'http',
        }
    )

    for line_item_id in line_item_ids:
        # GETting the line item without a Response object will update
        # the object's cache
        line_item = await interface.show(
            advertiser_id=brand.amazon.aa.dsp.advertiser_id,
            key=line_item_id,
            request=request,
        )
        line_items.append(line_item)
    
    log.info(
        f'Updated DSP line items',
    )

    return line_items
    

@router.delete(
    '/{line_item_id}',
    dependencies=[
        Depends(write),
    ],
)
async def destroy(
    request: Request,
    line_item_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Archiving DSP line item {line_item_id}...',
    )

    response = await interface.destroy(
        advertiser_id=brand.amazon.aa.dsp.entity_id,
        key=line_item_id,
        request=request,
    )

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/dsp/line_items',
            'query_string': b'',
            'type': 'http',
        }
    )

    # GETting the line item without a Response object will update
    # the object's cache
    line_item = await interface.show(
        advertiser_id=brand.amazon.aa.dsp.entity_id,
        key=line_item_id,
        request=request,
    )
    
    log.info(
        f'Archived DSP line item {line_item_id}',
    )

    return line_item


@router.post(
    '/{line_item_id}/delivery_activation_status',
    dependencies=[
        Depends(write),
    ],
)
async def create_delivery_activation_status(
    request: Request,
    line_item_id: str,
    status: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Creating delivery activation status for {line_item_id}...',
    )

    response = await interface.create_delivery_activation_status(
        advertiser_id=brand.amazon.aa.dsp.entity_id,
        key=line_item_id,
        status=status,
        request=request,
    )

    log.info(
        f'Created delivery activation status for {line_item_id}',
    )

    return response
