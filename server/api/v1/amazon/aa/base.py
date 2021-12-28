from fastapi import APIRouter

from server.core.constants import Constants
from server.api.v1.amazon.aa.dsp.base import router as DSPRouter
from server.api.v1.amazon.aa.portfolios.base import router as PortfolioRouter
from server.api.v1.amazon.aa.sb.base import router as SBRouter
from server.api.v1.amazon.aa.sd.base import router as SDRouter
from server.api.v1.amazon.aa.sp.base import router as SPRouter


router = APIRouter(
    prefix=Constants.AMAZON_ADVERTISING_PREFIX,
    tags=[Constants.AMAZON],
)

router.include_router(DSPRouter)
router.include_router(PortfolioRouter)
router.include_router(SBRouter)
router.include_router(SDRouter)
router.include_router(SPRouter)
