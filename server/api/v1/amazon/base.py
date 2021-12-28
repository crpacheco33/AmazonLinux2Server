from fastapi import APIRouter

from server.core.constants import Constants
from server.api.v1.amazon.aa.base import router as AARouter
from server.api.v1.amazon.ba.base import router as BARouter


router = APIRouter(
    prefix=Constants.AMAZON_PREFIX,
    tags=[Constants.AMAZON],
)

router.include_router(AARouter)
router.include_router(BARouter)
