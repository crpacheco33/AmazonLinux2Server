from fastapi import APIRouter

from server.core.constants import Constants
from server.api.v1.amazon.aa.dsp.creative import router as CreativeRouter
from server.api.v1.amazon.aa.dsp.dashboard import router as DashboardRouter
from server.api.v1.amazon.aa.dsp.discovery import router as DiscoveryRouter
from server.api.v1.amazon.aa.dsp.line_item_creative import router as LineItemCreativeRouter
from server.api.v1.amazon.aa.dsp.line_item import router as LineItemRouter
from server.api.v1.amazon.aa.dsp.order import router as OrderRouter


router = APIRouter(
    prefix=Constants.DSP_PREFIX,
    tags=[Constants.AMAZON],
)

router.include_router(CreativeRouter)
router.include_router(DashboardRouter)
router.include_router(DiscoveryRouter)
router.include_router(LineItemCreativeRouter)
router.include_router(LineItemRouter)
router.include_router(OrderRouter)
