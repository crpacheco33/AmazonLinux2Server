from typing import (
    Any,
)

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
    brand,
    Interface,
    read,
    sb_client,
)
from server.middleware.aa_middleware import AAMiddleware
from server.services.aws_service import AWSService
from server.utilities.aa_utility import AAUtility


klass = AAUtility().campaign_budget_rule_recommendation_interface_klass(
    Constants.SPONSORED_BRANDS,
)
interface = Interface(sb_client, klass)
aws_service = AWSService()
log = aws_service.log_service
router = APIRouter(
    prefix=Constants.RECOMMENDATIONS_PREFIX,
    route_class=AAMiddleware,
)


@router.get(
    Constants.BUDGET_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index(
    request: Request,
    response: Response,
    campaignId: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Indexing SB budget rules recommendations...',
    )

    response = await interface.create(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        request=request,
    )

    recommendations = response.get(
        'recommendedBudgetRuleEvents',
        [],
    )

    log.info(
        f'Indexed SB budget rules recommendations',
    )

    return recommendations
