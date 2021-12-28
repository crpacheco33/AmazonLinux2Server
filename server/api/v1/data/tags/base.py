from fastapi import APIRouter

from server.api.v1.data.tags.objectives import router as ObjectivesRouter
from server.core.constants import Constants


router = APIRouter(
    prefix=Constants.TAGS_PREFIX,
    tags=[Constants.TAGS],
)

router.include_router(ObjectivesRouter)
