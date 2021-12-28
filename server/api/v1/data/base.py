from fastapi import APIRouter

from server.api.v1.data.amazon.base import router as AmazonRouter
from server.api.v1.data.tags.base import router as TagRouter
from server.core.constants import Constants


router = APIRouter(
    prefix=Constants.DATA_PREFIX,
    tags=[Constants.DATA],
)

router.include_router(AmazonRouter)
router.include_router(TagRouter)