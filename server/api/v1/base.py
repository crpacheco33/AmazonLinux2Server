from fastapi import APIRouter

from server.api.v1.amazon.base import router as AmazonRouter
from server.api.v1.auth import router as AuthRouter
from server.api.v1.brands import router as BrandsRouter
from server.api.v1.data.base import router as DataRouter
from server.api.v1.dayparts import router as DaypartsRouter
from server.api.v1.insights import router as InsightsRouter
from server.api.v1.reports import router as ReportsRouter
from server.api.v1.status import router as StatusRouter
from server.api.v1.tags import router as TagsRouter
from server.api.v1.users import router as UsersRouter


router = APIRouter()

router.include_router(AmazonRouter)
router.include_router(AuthRouter)
router.include_router(BrandsRouter)
router.include_router(DataRouter)
router.include_router(DaypartsRouter)
router.include_router(InsightsRouter)
router.include_router(ReportsRouter)
router.include_router(StatusRouter)
router.include_router(TagsRouter)
router.include_router(UsersRouter)
