from typing import (
    Any,
    List,
)

from bson.objectid import ObjectId
from fastapi import (
    APIRouter,
    Request,
)
from fastapi.params import (
    Depends,
)
from starlette.responses import JSONResponse
from starlette.status import HTTP_401_UNAUTHORIZED

import pymongo

from server.core.constants import Constants
from server.dependencies import (
    auth_service,
    docdb,
    read,
    ssm_service,
    user,
)
from server.resources.models.brand import Brand
from server.resources.schema.brand import (
    BrandIndexSchema,
    BrandShowSchema,
)
from server.services.auth_service import AuthService
from server.services.aws_service import AWSService


log = AWSService().log_service
router = APIRouter(
    prefix=Constants.BRANDS_PREFIX,
    tags=[Constants.BRANDS],
)


@router.get(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(read),
    ],
    response_model=List[BrandIndexSchema],
    response_model_by_alias=False,
)
async def index(
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    user: Any = Depends(
        user,
    ),
):
    log.info(
        'Indexing brands...',
    )

    brands = Brand.with_user(user._id, client)
    
    log.info(
        'Indexed brands',
    )

    return brands


@router.get(
    '/{brand_id}',
    dependencies=[
        Depends(read),
    ],
    response_model=BrandShowSchema,
    response_model_by_alias=False,
)
async def show(
    brand_id: str,
    authenticator: AuthService = Depends(
        auth_service,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    user: Any = Depends(
        user,
    ),
):
    log.info(
        f'Showing brand {brand_id}...',
    )

    brand = Brand.find_by_id(
        ObjectId(brand_id),
        client,
    )

    brand = Brand.find_by_id_and_user(
        ObjectId(brand_id),
        user._id,
        client,
    )

    log.info(
        f'Showed brand {brand_id}',
    )

    return brand


@router.put(
    '/{brand_id}',
    dependencies=[
        Depends(read),
    ],
)
async def update(
    brand_id: str,
    authenticator: AuthService = Depends(
        auth_service,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    ssm_service: AWSService.SSMService = Depends(
        ssm_service,
    ),
    user: Any = Depends(
        user,
    ),
):
    log.info(
        'Updating brand...',
    )

    try:
        access_token, expires_in, refresh_token = authenticator.authenticate_for_brand(
            brand_id,
            user,
            client,
        )
    except Exception as e:
        log.exception(e)
        return HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail=str(e),
        )
    
    if access_token:
        response = JSONResponse({
            'access_token': access_token,
            'expires_in': expires_in,
        })
        response.set_cookie(
            domain=ssm_service.cookie_domain,
            expires=Constants.REFRESH_TOKEN_DURATION,
            httponly=True,
            key=Constants.REFRESH_TOKEN_COOKIE_KEY,
            samesite=Constants.STRICT,
            value=refresh_token,
        )

        log.info(
            'Updated brand',
        )

        return response
