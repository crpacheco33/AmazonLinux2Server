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
from server.services.aws_service import AWSService
from server.services.data_service import DataService
from server.utilities.aa_utility import AAUtility
from server.utilities.list_utility import partition_list


klass = AAUtility().creative_interface_klass()
interface = Interface(dsp_client, klass)
log = AWSService().log_service
router = APIRouter(
    prefix=Constants.CREATIVES_PREFIX,
    route_class=AAMiddleware,
)


@router.get(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(read),
    ],
    response_model_by_alias=True,
)
async def index(
    request: Request,
    response: Response,
    from_date: str = None,
    to_date: str = None,
    creativeIdFilter: str = None,
    creativeLineItemIdFilter: str = None,
    lineItemTypeFilter: str = None,
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
        f'Indexing DSP creatives...',
    )

    creatives = await interface.index(
        advertiser_id=brand.amazon.aa.dsp.advertiser_id,
        request=request,
        response=response,
    )
    total = len(creatives)

    creative_ids = [creative.get('creativeId') for creative in creatives]

    data = {}
    partitions = partition_list(creative_ids, Constants.ES_FILTER_ARRAY_LIMIT)

    for partition in partitions:
        es_data = source.dsp_model(
            Constants.DSP,
            Constants.CREATIVE,
            partition,
            from_date,
            to_date,
        )
        data.update(es_data)

    response = []

    for creative in creatives:
        creative_id = creative.get('creativeId')
        if creative_id in data:
            creative.update(
                data.get(creative_id)
            )

        response.append(creative)
    
    log.info(
        f'Indexed DSP creatives',
    )

    return { 'data': response }

    # return APIIndexSchema(
    #     data=response,
    #     total_count=total,
    # )
