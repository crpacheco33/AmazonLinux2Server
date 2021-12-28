from fastapi import APIRouter

from server.api.v1.data.amazon.advertising import router as AdvertisingRouter
from server.api.v1.data.amazon.my_dashboard import router as MyDashboardRouter
from server.api.v1.data.amazon.objectives import router as ObjectivesRouter
from server.core.constants import Constants


router = APIRouter(
    prefix=Constants.AMAZON_PREFIX,
    tags=[Constants.AMAZON],
)

router.include_router(AdvertisingRouter)
router.include_router(MyDashboardRouter)
router.include_router(ObjectivesRouter)
