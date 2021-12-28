from fastapi import APIRouter

from server.core.constants import Constants
from server.api.v1.amazon.aa.portfolios.dashboard import router as DashboardRouter
from server.api.v1.amazon.aa.portfolios.portfolio import router as PortfolioRouter


router = APIRouter(
    tags=[Constants.AMAZON],
)

router.include_router(DashboardRouter)
router.include_router(PortfolioRouter)
