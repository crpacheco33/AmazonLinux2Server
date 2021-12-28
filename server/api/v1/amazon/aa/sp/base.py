from fastapi import APIRouter

from server.core.constants import Constants
from server.api.v1.amazon.aa.sp.ad_group import router as AdGroupRouter
from server.api.v1.amazon.aa.sp.campaign import router as CampaignRouter
from server.api.v1.amazon.aa.sp.dashboard import router as DashboardRouter
from server.api.v1.amazon.aa.sp.keyword import router as KeywordRouter
from server.api.v1.amazon.aa.sp.product_ad import router as ProductAdRouter
from server.api.v1.amazon.aa.sp.target import router as TargetRouter


router = APIRouter(
    prefix=Constants.SPONSORED_PRODUCTS_PREFIX,
    tags=[Constants.AMAZON],
)

router.include_router(AdGroupRouter)
router.include_router(CampaignRouter)
router.include_router(DashboardRouter)
router.include_router(KeywordRouter)
router.include_router(ProductAdRouter)
router.include_router(TargetRouter)
