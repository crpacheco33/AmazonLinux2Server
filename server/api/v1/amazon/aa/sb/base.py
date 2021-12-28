from fastapi import APIRouter

from server.core.constants import Constants
from server.api.v1.amazon.aa.sb.ad_group import router as AdGroupRouter
from server.api.v1.amazon.aa.sb.campaign import router as CampaignRouter
from server.api.v1.amazon.aa.sb.dashboard import router as DashboardRouter
from server.api.v1.amazon.aa.sb.keyword import router as KeywordRouter
from server.api.v1.amazon.aa.sb.target import router as TargetRouter


router = APIRouter(
    prefix=Constants.SPONSORED_BRANDS_PREFIX,
    tags=[Constants.AMAZON],
)

router.include_router(AdGroupRouter)
router.include_router(CampaignRouter)
router.include_router(DashboardRouter)
router.include_router(KeywordRouter)
router.include_router(TargetRouter)
