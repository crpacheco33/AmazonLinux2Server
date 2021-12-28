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

from server.core.constants import Constants
from server.dependencies import (
    brand,
    Interface,
    read,
    sp_client,
)
from server.middleware.aa_middleware import AAMiddleware
from server.services.aws_service import AWSService
from server.utilities.aa_utility import AAUtility


klass = AAUtility().budget_rule_interface_klass(
    Constants.SPONSORED_PRODUCTS,
)
interface = Interface(sp_client, klass)
aws_service = AWSService()
log = aws_service.log_service
router = APIRouter(
    prefix=Constants.BUDGET_RULES_PREFIX,
    route_class=AAMiddleware,
)


@router.post(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(write),
    ],
)
async def create(
    request: Request,
    response: Response,
    data: List[Any],
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    )
):
    log.info(
        'Creating SP budget rule...',
    )

    response = await interface.create(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        data=data,
        request=request,
    )

    log.info(
        'Created SP budget rule',
    )

    return response


@router.get(
    '/{budget_rule_id}',
    dependencies=[
        Depends(read),
    ],
)
async def show(
    request: Request,
    response: Response,
    budget_rule_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    )
):
    log.info(
        f'Showing SP budget rule {budget_rule_id}...',
    )

    response = await interface.show(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=budget_rule_id,
        request=request,
    )

    budget_rule = response.get(
        'budgetRule',
        {},
    )

    log.info(
        f'Showed SP budget rule {budget_rule_id}',
    )

    return budget_rule


@router.put(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(write),
    ],
)
async def update(
    request: Request,
    response: Response,
    data: List[Any],
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    )
):
    log.info(
        'Updating SP budget rule...',
    )

    response = await interface.update(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        data=data,
        request=request,
    )

    log.info(
        'Updated SP budget rule',
    )

    return response
