from typing import (
    Any,
    List,
)

import json

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

from server.core.constants import Constants
from server.dependencies import (
    advertising_data,
    brand,
    Interface,
    read,
    sp_client,
    write,
)
from server.middleware.aa_middleware import AAMiddleware
from server.resources.schema.amazon_api import (
    APIIndexSchema,
    UpdateSPTargetSchema,
)
from server.services.aws_service import AWSService
from server.services.data_service import DataService
from server.utilities.aa_utility import AAUtility
from server.utilities.list_utility import partition_list


klass = AAUtility().target_interface_klass(
    Constants.SPONSORED_PRODUCTS,
)
interface = Interface(sp_client, klass)
log = AWSService().log_service
router = APIRouter(
    prefix=Constants.TARGETS_PREFIX,
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
    adGroupIdFilter: str = None,
    targetIdFilter: str = None,
    expressionType: str = None,
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
        f'Indexing SP targets...',
    )

    targets = await interface.index(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=response,
    )
    total = len(targets)
    
    target_ids = [str(target.get('targetId')) for target in targets]

    data = {}
    partitions = partition_list(target_ids, Constants.ES_FILTER_ARRAY_LIMIT)

    for partition in partitions:
        es_data = source.sa_model(
            Constants.SPONSORED_PRODUCTS,
            Constants.TARGET,
            partition,
            from_date,
            to_date,
        )
        data.update(es_data)

    response = []

    for target in targets:
        target_id = str(target.get('targetId'))
        if target_id in data:
            target.update(
                data.get(target_id),
            )
        
        response.append(target)

    log.info(
        f'Indexed SP targets',
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
    '/{target_id}',
    dependencies=[
        Depends(read),
    ],
)
async def show(
    request: Request,
    response: Response,
    target_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Showing SP target {target_id}...',
    )

    response = await interface.show(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=target_id,
        request=request,
        response=response,
    )
    
    log.info(
        f'Showed SP target {target_id}',
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
    data: List[UpdateSPTargetSchema],
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Updating SP targets...',
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

    targets, target_ids = [], []
    try:
        target_ids = [target.get('targetId') for target in response]
    except AttributeError as e:
        log.exception(e)
        log.info(response)
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(response),
        )

        return response

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sp/targets',
            'query_string': b'',
            'type': 'http',
        }
    )

    for target_id in target_ids:
        # GETting the target without a Response object will update
        # the object's cache
        target = await interface.show(
            advertiser_id=brand.amazon.aa.sa.advertiser_id,
            key=target_id,
            request=request,
        )
        targets.append(target)

    log.info(
        f'Updated SP targets',
    )
    
    return targets


@router.delete(
    '/{target_id}',
    dependencies=[
        Depends(write),
    ],
)
async def destroy(
    request: Request,
    target_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Archiving SP target {target_id}...',
    )

    response = await interface.destroy(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=target_id,
        request=request,
    )

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sp/targets',
            'query_string': b'',
            'type': 'http',
        }
    )

    # GETting the target without a Response object will update
    # the object's cache
    target = await interface.show(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=target_id,
        request=request,
    )

    log.info(
        f'Archived SP target {target_id}',
    )

    return target
