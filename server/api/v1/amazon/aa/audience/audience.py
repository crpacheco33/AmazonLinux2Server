from fastapi import (
    APIRouter,
    Request,
)
from fastapi.params import (
    Depends,
)

from server.core.constants import Constants
from server.delegates.aa_delegate import AADelegate
from server.dependencies import (
    brand,
    dsp_client,
    Interface,
    read,
)
from server.middleware.aa_middleware import AAMiddleware
from server.services.api_service import APIService
from server.services.aws_service import AWSService
from server.utilities.aa_utility import AAUtility


klass = AAUtility().audience_interface_klass()
interface = Interface(dsp_client, klass)
log = AWSService().log_service
router = APIRouter(
    prefix=Constants.NO_PREFIX,
    route_class=AAMiddleware,
)


@router.post(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
def index_create(
    request: Request,
    data: dict = Body(...),
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Indexing audiences...',
    )

    response = await interface.index_create(
        brand.amazon.aa.dsp.entity_id,
        data=data,
        request=request,
    )

    log.info(
        f'Indexed audiences',
    )

    return response
    