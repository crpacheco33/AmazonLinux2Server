from fastapi import APIRouter

from server.core.constants import Constants
from server.api.v1.amazon.aa.sd.ad_group import router as AdGroupRouter
from server.api.v1.amazon.aa.sd.campaign import router as CampaignRouter
from server.api.v1.amazon.aa.sd.dashboard import router as DashboardRouter
from server.api.v1.amazon.aa.sd.product_ad import router as ProductAdRouter
from server.api.v1.amazon.aa.sd.target import router as TargetRouter


router = APIRouter(
    prefix=Constants.SPONSORED_DISPLAY_PREFIX,
    tags=[Constants.AMAZON],
)

router.include_router(AdGroupRouter)
router.include_router(CampaignRouter)
router.include_router(DashboardRouter)
router.include_router(ProductAdRouter)
router.include_router(TargetRouter)
