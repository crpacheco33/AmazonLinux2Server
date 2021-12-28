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
    LineItemCreativeAssociation,
    LineItemCreativeAssociationsRequest,
)
from server.services.aws_service import AWSService
from server.services.data_service import DataService
from server.utilities.aa_utility import AAUtility


klass = AAUtility().line_item_creative_association_interface_klass()
interface = Interface(dsp_client, klass)
log = AWSService().log_service
router = APIRouter(
    prefix=Constants.LINE_ITEM_CREATIVE_ASSOCIATION_PREFIX,
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
    creativeIdFilter: str = None,
    creativeLineItemIdFilter: str = None,
    lineItemIdFilter: str = None,
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
        f'Indexing DSP line item creative associations...',
    )

    response = await interface.index(
        advertiser_id=brand.amazon.aa.dsp.advertiser_id,
        request=request,
        response=response,
    )

    log.info(
        f'Indexed DSP line item creative associations',
    )

    return response


@router.post(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(write),
    ],
)
async def create(
    data: LineItemCreativeAssociationsRequest,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Creating DSP line item creative associations...',
    )

    data = json.loads(data.json())

    response = await interface.create(
        advertiser_id=brand.amazon.aa.dsp.advertiser_id,
        data=data,
    )

    log.info(
        f'Created DSP line item creative associations',
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
    data: LineItemCreativeAssociation,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Updating DSP line item creative associations...',
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
    )
    
    associations = []
    association_ids = [association.get('lineItemId') for association in response]

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/dsp/line_item_creative_associations',
            'query_string': f'lineItemIdFilter={association_ids[0]}',
            'type': 'http',
        }
    )

    # GETting the assocations without a Response object will update
    # the object's cache
    associations = await interface.index(
        advertiser_id=brand.amazon.aa.dsp.advertiser_id,
        request=request,
    )
        
    log.info(
        f'Updated DSP line item creative associations',
    )

    return associations