import json

from typing import (
    Any,
    List,
)

from fastapi import (
    APIRouter,
    HTTPException,
    Request,
    Response,
)
from fastapi.params import (
    Depends,
)
from starlette.status import (
    HTTP_422_UNPROCESSABLE_ENTITY,
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
    Order,
)
from server.services.aws_service import AWSService
from server.services.data_service import DataService
from server.utilities.aa_utility import AAUtility
from server.utilities.list_utility import partition_list


klass = AAUtility().order_interface_klass()
interface = Interface(dsp_client, klass)
log = AWSService().log_service
router = APIRouter(
    prefix=Constants.ORDERS_PREFIX,
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
    name: str = None,
    query: str = None,
    from_date: str = None,
    to_date: str = None,
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
        f'Indexing DSP orders...',
    )

    orders = await interface.index(
        advertiser_id=brand.amazon.aa.dsp.advertiser_id,
        request=request,
        response=response,
    )
    total = len(orders)

    order_ids = [order.get('orderId') for order in orders]

    data = {}
    partitions = partition_list(order_ids, Constants.ES_FILTER_ARRAY_LIMIT)

    for partition in partitions:
        es_data = source.dsp_model(
            Constants.DSP,
            Constants.ORDER,
            partition,
            from_date,
            to_date,
        )
        data.update(es_data)

    response = []

    for order in orders:
        order_id = order.get('orderId')
        if order_id in data:
            order.update(
                data.get(order_id)
            )

        response.append(order)
    
    log.info(
        f'Indexed DSP orders',
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
    '/{order_id}',
    dependencies=[
        Depends(read),
    ],
)
async def show(
    request: Request,
    response: Response,
    order_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Showing DSP order {order_id}...',
    )

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/dsp/orders',
            'query_string': b'',
            'type': 'http',
        }
    )

    response = await interface.show(
        advertiser_id=brand.amazon.aa.dsp.entity_id,
        key=order_id,
        request=request,
    )

    if isinstance(response, list):
        response = response[0]

    log.info(
        f'Showed DSP order {order_id}',
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
    data: List[Order],
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Updating DSP orders...',
    )

    order_data = []
    for datum in data:
        order_datum = json.loads(
            datum.json(
                exclude_none=True,
                exclude_defaults=True,
            )
        )
        order_datum.pop('agencyFee')
        order_datum['advertiserId'] = brand.amazon.aa.dsp.advertiser_id

        budget_caps = order_datum.get('budget', {}).get('budgetCaps', [])
        if len(budget_caps):
            for budget_cap in budget_caps:
                if budget_cap.get('recurrenceTimePeriod') == 'UNCAPPED':
                    budget_cap.pop('amount', None)

        order_data.append(order_datum)

    log.info(
        f'Updating orders using {order_data}...'
    )

    response = await interface.update(
        advertiser_id=brand.amazon.aa.dsp.entity_id,
        data=order_data,
        request=request,
    )

    log.info(response)

    orders, order_ids = [], []
    try:
        order_ids = [order.get('orderId') for order in response]
    except AttributeError as e:
        log.exception(e)
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(response),
        )

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/dsp/orders',
            'query_string': b'',
            'type': 'http',
        }
    )

    for order_id in order_ids:
        # GETting the order without a Response object will update
        # the object's cache
        order = await interface.show(
            advertiser_id=brand.amazon.aa.dsp.entity_id,
            dsp_advertiser_id=brand.amazon.aa.dsp.advertiser_id,
            key=order_id,
            request=request,
        )
        orders.append(order)
    
    log.info(
        f'Updated DSP orders',
    )

    return orders
    

@router.delete(
    '/{order_id}',
    dependencies=[
        Depends(write),
    ],
)
async def destroy(
    request: Request,
    order_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Archiving DSP order {order_id}...',
    )

    response = await interface.destroy(
        advertiser_id=brand.amazon.aa.dsp.entity_id,
        key=order_id,
        request=request,
    )

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/dsp/orders',
            'query_string': b'',
            'type': 'http',
        }
    )

    # GETting the order without a Response object will update
    # the object's cache
    order = await interface.show(
        advertiser_id=brand.amazon.aa.dsp.advertiser_id,
        key=order_id,
        request=request,
    )
    
    log.info(
        f'Archived DSP order {order_id}',
    )

    return order


@router.post(
    '/{order_id}/delivery_activation_status',
    dependencies=[
        Depends(write),
    ],
)
async def create_delivery_activation_status(
    request: Request,
    order_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Creating delivery activation status for {order_id}...',
    )

    response = await interface.create_delivery_activation_status(
        advertiser_id=brand.amazon.aa.dsp.advertiser_id,
        key=order_id,
        request=request,
    )
    
    log.info(
        f'Created delivery activation status for {order_id}',
    )

    return response


@router.get(
    '/{order_id}/conversion_tracking',
    dependencies=[
        Depends(read),
    ],
)
async def show_conversion_tracking(
    request: Request,
    order_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Showing conversion tracking for {order_id}...',
    )

    response = await interface.show_conversion_tracking(
        advertiser_id=brand.amazon.aa.dsp.advertiser_id,
        key=order_id,
        request=request,
    )
    
    log.info(
        f'Showed conversion tracking for {order_id}',
    )

    return response


@router.put(
    '/{order_id}/conversion_tracking',
    dependencies=[
        Depends(write),
    ],
)
async def update_conversion_tracking(
    request: Request,
    order_id: str,
    data: Any,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Updating conversion tracking for {order_id}...',
    )

    data = json.loads(
        exclude_none=True,
        exclude_defaults=True,
    )

    response = await interface.update_conversion_tracking(
        advertiser_id=brand.amazon.aa.dsp.advertiser_id,
        key=order_id,
        data=data,
    )
    
    log.info(
        f'Updated conversion tracking for {order_id}',
    )

    return response
