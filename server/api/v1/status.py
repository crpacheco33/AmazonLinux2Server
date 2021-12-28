from fastapi import APIRouter

from server.core.constants import Constants


router = APIRouter(
    prefix=Constants.STATUS_PREFIX,
    tags=[Constants.STATUS],
)


@router.get(Constants.NO_PREFIX)
async def status():
    return {
        Constants.STATUS: Constants.ALIVE,
    }