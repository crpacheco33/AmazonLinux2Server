from fastapi import APIRouter
from fastapi.params import (
    Cookie,
    Depends,
)

from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_403_FORBIDDEN,
)

import pymongo

from server.core.constants import Constants
from server.dependencies import (
    user,
    docdb,
)
from server.resources.models.user import User
from server.resources.schema.user import (
    UserPasswordUpdateSchema,
)
from server.utilities.auth_utility import AuthUtility


router = APIRouter(
    prefix=Constants.ACCOUNTS_PREFIX,
    tags=[Constants.ACCOUNTS],
)


@router.patch('/{user_id}')
async def update_password(
    user_id: str,
    data: UserPasswordUpdateSchema,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    user: User = Depends(
        user,
    ),
):
    if user_id != str(user._id):
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail=Constants.USER_FORBIDDEN,
        )

    auth_utility = AuthUtility()
    is_valid_password = auth_utility.authenticate_password(
        data.password,
        user.password,
    )

    if is_valid_password:
        try:
            encrypted_password = auth_utility.encrypt_password(
                data.new_password,
            )
            with client.start_session() as session:
                collection = client.visibly.users
                collection.update(
                    { '_id': user._id },
                    { '$set': { 'password': encrypted_password } },
                )
            return
        except Exception as e:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=str(e),
            )

    raise HTTPException(
        status_code=HTTP_400_BAD_REQUEST,
        detail=Constants.USER_INVALID_PASSWORD,
    )