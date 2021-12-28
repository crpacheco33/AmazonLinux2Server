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

from server.core.constants import Constants
from server.dependencies import (
    advertising_data,
    brand,
    Interface,
    read,
    sd_client,
    write,
)
from server.middleware.aa_middleware import AAMiddleware
from server.resources.schema.amazon_api import (
    APIIndexSchema,
    UpdateSDProductAdSchema,
)
from server.services.aws_service import AWSService
from server.services.data_service import DataService
from server.utilities.aa_utility import AAUtility
from server.utilities.list_utility import partition_list


klass = AAUtility().product_ad_interface_klass(
    Constants.SPONSORED_DISPLAY,
)
interface = Interface(sd_client, klass)
log = AWSService().log_service
router = APIRouter(
    prefix=Constants.PRODUCT_ADS_PREFIX,
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
    adIdFilter: str = None,
    campaignIdFilter: str = None,
    stateFilter: str = None,
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
        f'Indexing SD product ads...',
    )

    product_ads = await interface.index(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=response,
    )
    total = len(product_ads)
    
    product_ad_ids = [str(product_ad.get('adId')) for product_ad in product_ads]

    data = {}
    partitions = partition_list(product_ad_ids, Constants.ES_FILTER_ARRAY_LIMIT)

    for partition in partitions:
        es_data = source.sa_model(
            Constants.SPONSORED_DISPLAY,
            Constants.PRODUCT_AD,
            partition,
            from_date,
            to_date,
        )
        data.update(es_data)

    response = []

    for product_ad in product_ads:
        product_ad_id = str(product_ad.get('adId'))
        if product_ad_id in data:
            product_ad.update(
                data.get(product_ad_id),
            )
        
        response.append(product_ad)

    log.info(
        f'Indexed SD product ads',
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
    '/{product_ad_id}',
    dependencies=[
        Depends(read),
    ],
)
async def show(
    request: Request,
    response: Response,
    product_ad_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Showing SD product ad {product_ad_id}...',
    )

    response = await interface.show(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=product_ad_id,
        request=request,
        response=response,
    )
    
    log.info(
        f'Showed SD product ad {product_ad_id}',
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
    data: List[UpdateSDProductAdSchema],
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Updating SD product ads...',
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

    product_ads = []
    product_ads_ids = [product_ad.get('adId') for product_ad in response]

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sd/product_ads',
            'query_string': b'',
            'type': 'http',
        }
    )

    for ad_id in product_ads_ids:
        # GETting the product ad without a Response object will update
        # the object's cache
        product_ad = await interface.show(
            advertiser_id=brand.amazon.aa.sa.advertiser_id,
            key=ad_id,
            request=request,
        )
        product_ads.append(product_ad)

    log.info(
        f'Updated SD product ads',
    )

    return product_ads


@router.delete(
    '/{product_ad_id}',
    dependencies=[
        Depends(write),
    ],
)
async def destroy(
    request: Request,
    product_ad_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Archiving SD product ad {product_ad_id}...',
    )

    response = await interface.destroy(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=product_ad_id,
        request=request,
    )

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sd/product_ads',
            'query_string': b'',
            'type': 'http',
        }
    )

    # GETting the product ad without a Response object will update
    # the object's cache
    product_ad = await interface.show(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=product_ad_id,
        request=request,
    )

    log.info(
        f'Archived SD product ad {product_ad_id}',
    )

    return product_ad
