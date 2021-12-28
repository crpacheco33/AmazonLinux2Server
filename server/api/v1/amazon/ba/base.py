from fastapi import APIRouter

from server.core.constants import Constants
from server.api.v1.amazon.ba.report import router as ReportsRouter
from server.api.v1.amazon.ba.search_terms import router as SearchTermsRouter


router = APIRouter(
    prefix=Constants.BRAND_ANALYTICS_PREFIX,
    tags=[Constants.AMAZON],
)

router.include_router(ReportsRouter)
router.include_router(SearchTermsRouter)
