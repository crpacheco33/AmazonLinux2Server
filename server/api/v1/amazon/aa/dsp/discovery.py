from typing import (
    Any,
    List,
    Optional,
)

from fastapi import (
    APIRouter,
    Request,
    Response,
)
from fastapi.params import (
    Depends,
    Query,
)

from server.core.constants import Constants
from server.dependencies import (
    brand,
    dsp_client,
    Interface,
    read,
)
from server.middleware.aa_middleware import AAMiddleware
from server.resources.schema.amazon_api import (
    APIIndexSchema,
)
from server.resources.schema.dsp_schema import (
    DeviceTypeTargeting,
    LineItemType,
    ProductLocation,
    SupplySourceType,
)
from server.services.aws_service import AWSService
from server.utilities.aa_utility import AAUtility


aa_utility = AAUtility()
app_klass = aa_utility.app_interface_klass()
app_interface = Interface(dsp_client, app_klass)

domain_list_klass = aa_utility.domain_list_interface_klass()
domain_list_interface = Interface(dsp_client, domain_list_klass)

geo_location_klass = aa_utility.geo_location_interface_klass()
geo_location_interface = Interface(dsp_client, geo_location_klass)

goal_configuration_klass = aa_utility.goal_configuration_interface_klass()
goal_configuration_interface = Interface(dsp_client, goal_configuration_klass)

iab_content_category_klass = aa_utility.iab_content_category_interface_klass()
iab_content_category_interface = Interface(dsp_client, iab_content_category_klass)

pixel_klass = aa_utility.pixel_interface_klass()
pixel_interface = Interface(dsp_client, pixel_klass)

pre_bid_targeting_klass = aa_utility.pre_bid_targeting_interface_klass()
pre_bid_targeting_interface = Interface(dsp_client, pre_bid_targeting_klass)

product_category_klass = aa_utility.product_category_interface_klass()
product_category_interface = Interface(dsp_client, product_category_klass)

supply_source_klass = aa_utility.supply_source_interface_klass()
supply_source_interface = Interface(dsp_client, supply_source_klass)

log = AWSService().log_service
router = APIRouter(
    # prefix=Constants.DISCOVERY_PREFIX,
    route_class=AAMiddleware,
)


@router.get(
    Constants.APPS_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index_apps(
    request: Request,
    response: Response,
    appsIdFilter: str = None,
    textQuery: str = None,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        app_interface,
    ),
):
    log.info(
        f'Indexing apps...',
    )

    response = await interface.index(
        advertiser_id=brand.amazon.aa.dsp.entity_id,
        request=request,
        response=response,
    )

    log.info(
        f'Indexed apps',
    )

    return response


@router.get(
    Constants.DOMAIN_LIST_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index_domain_lists(
    request: Request,
    response: Response,
    nextToken: Optional[str] = None,
    maxResults: Optional[int] = None,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        domain_list_interface,
    ),
):
    log.info(
        f'Indexing domain lists...',
    )

    response = await interface.index(
        advertiser_id=brand.amazon.aa.dsp.entity_id,
        request=request,
        response=response,
    )

    log.info(
        f'Indexed domain lists',
    )

    return response


@router.get(
    Constants.GEO_LOCATIONS_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index_geo_locations(
    request: Request,
    response: Response,
    geoLocationIdFilter: Optional[str] = None,
    textQuery: Optional[str] = None,
    nextToken: Optional[str] = None,
    maxResults: Optional[int] = None,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        geo_location_interface,
    ),
):
    log.info(
        f'Indexing geo locations...',
    )

    response = await interface.index(
        advertiser_id=brand.amazon.aa.dsp.entity_id,
        request=request,
        response=response,
    )

    log.info(
        f'Indexed geo locations',
    )

    return response


@router.get(
    Constants.GOAL_CONFIGURATIONS_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index_goal_configurations(
    request: Request,
    response: Response,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        goal_configuration_interface,
    ),
):
    log.info(
        f'Indexing goal configurations...',
    )

    response = await interface.index(
        advertiser_id=brand.amazon.aa.dsp.entity_id,
        request=request,
        response=response,
    ).json()

    log.info(
        f'Indexed goal configurations',
    )

    return response


@router.get(
    Constants.IAB_CONTENT_CATEGORIES_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index_iab_content_categories(
    request: Request,
    response: Response,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        iab_content_category_interface,
    ),
):
    log.info(
        f'Indexing IAB content categories...',
    )

    response = await interface.index(
        advertiser_id=brand.amazon.aa.dsp.entity_id,
        request=request,
        response=response,
    )

    log.info(
        f'Indexed IAB content categories',
    )

    return response


@router.get(
    Constants.PIXELS_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index_pixels(
    request: Request,
    response: Response,
    pixelIdFilter: Optional[str] = None,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        pixel_interface,
    ),
):
    log.info(
        f'Indexing pixels...',
    )

    response = await interface.index(
        advertiser_id=brand.amazon.aa.dsp.advertiser_id,
        request=request,
        response=response,
    )

    log.info(
        f'Indexed pixels',
    )

    return response


@router.get(
    Constants.PRE_BID_TARGETING_DV_CUSTOM_CONTEXTUAL_SEGMENTS_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def show_pre_bid_targeting_dv_custom_contextual_segments(
    request: Request,
    response: Response,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        pre_bid_targeting_interface,
    ),
):
    log.info(
        f'Indexing prebid targeting...',
    )

    response = await interface.show(
        advertiser_id=brand.amazon.aa.dsp.advertiser_id,
        provider=Constants.DOUBLE_VERIFY_PROVIDER,
        request=request,
        response=response,
    )
    
    log.info(
        f'Indexed prebid targeting',
    )

    return response


@router.get(
    Constants.PRODUCT_CATEGORIES_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index_product_categories(
    request: Request,
    response: Response,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        product_category_interface,
    ),
):
    log.info(
        f'Indexing product categories...',
    )

    response = await interface.index(
        advertiser_id=brand.amazon.aa.dsp.entity_id,
        request=request,
        response=response,
    )

    log.info(
        f'Indexed product categories',
    )

    return response


@router.get(
    Constants.SUPPLY_SOURCES_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index_supply_sources(
    request: Request,
    response: Response,
    lineItemType: LineItemType,
    supplySourceType: SupplySourceType,
    deviceTypes: Optional[str] = None,
    orderId: str = None,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        supply_source_interface,
    ),
):
    log.info(
        f'Indexing supply sources...',
    )

    if supplySourceType == SupplySourceType.DEAL:
        response = await interface.index(
            advertiser_id=brand.amazon.aa.dsp.entity_id,
            request=request,
        )
        try:
            response = {
                'supplySources': response.json().get('supplySources', []),
            }  # Remove other keys from the response (if any)
        except Exception as e:
            log.exception(e)
            response = { 'supplySources': [] }
    else:
        response = await interface.index(
            advertiser_id=brand.amazon.aa.dsp.entity_id,
            request=request,
            response=response,
        )

        response = { 'supplySources': response }

    log.info(
        f'Indexed supply sources',
    )

    return response
