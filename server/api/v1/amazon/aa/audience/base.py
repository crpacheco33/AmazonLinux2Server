from fastapi import APIRouter

from server.core.constants import Constants
from server.api.v1.amazon.aa.audience.audience import router as AudienceRouter


router = APIRouter(
    prefix=Constants.AUDIENCES_PREFIX,
    tags=[Constants.AMAZON],
)

router.include_router(AudienceRouter)
