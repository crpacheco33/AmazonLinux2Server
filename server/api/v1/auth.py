"""
Routes – Authentication
=======================
"""
from typing import Optional

import uuid

from fastapi import (
    APIRouter,
    Response,
)
from fastapi.params import (
    Cookie,
    Depends,
)
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse
from starlette.status import (
    HTTP_200_OK,
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

import pymongo

from server.core.constants import Constants
from server.dependencies import (
    auth_service,
    docdb,
    ssm_service,
)
from server.resources.schema.user import (
    PasswordWantsResetSchema,
    PasswordWillResetSchema,
    ResendEmailSchema,
    User2FASchema,
    UserLoginSchema,
    UserInvitationSchema,
)
from server.services.auth_service import AuthService
from server.services.aws_service import AWSService


log = AWSService().log_service
router = APIRouter(
    prefix=Constants.AUTH_PREFIX,
    tags=[Constants.AUTH],
)


@router.post(Constants.CONFIRM_PREFIX)
async def authenticate(
    data: User2FASchema,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    authenticator: AuthService = Depends(
        auth_service,
    ),
    ssm_service: AWSService.SSMService = Depends(
        ssm_service,
    ),
):
    """Summary line.

    Extended description of function.

    Args:
        data: authentication request
        client: database client
        authenticator: instance of AuthService

    Returns:
        None

    """
    try:
        access_token, expires_in, refresh_token = authenticator.authenticate(
            data,
            client,
        )
    except Exception as e:
        log.exception(e)
        raise HTTPException(
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
    else:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail=Constants.INVALID_CREDENTIALS,
        )

    return response


@router.post(
    Constants.TOKEN_PREFIX,
)
async def refresh(
    refresh_token: Optional[str] = Cookie(None),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    authenticator: AuthService = Depends(
        auth_service,
    ),
    ssm_service: AWSService.SSMService = Depends(
        ssm_service,
    ),
):
    log.info(
        'Refreshing token...',
    )

    if refresh_token is None:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail=Constants.REFRESH_TOKEN_MISSING,
        )

    try:
        access_token, expires_in, refresh_token = authenticator.refresh_token(
            refresh_token,
            client,
        )

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
            'Refreshed token',
        )

        return response
    except ValueError as e:
        log.error(e)
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail=Constants.REFRESH_TOKEN_MISSING_EMAIL,
        )


@router.put(Constants.INVITE_PREFIX)
async def register(
    data: UserInvitationSchema,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    authenticator = Depends(
        auth_service,
    ),
):
    try:
        response = authenticator.register(
            data,
            client,
        )
    except Exception as e:
        log.exception(e)

        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    return response


@router.post(Constants.RESEND_EMAIL_PREFIX)
async def wants_resend_email(
    data: ResendEmailSchema,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    authenticator = Depends(
        auth_service,
    )
):
    try:
        authenticator.wants_resend_email(
            data,
            client,
        )
    except Exception as e:
        log.exception(e)

        raise HTTPException(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        )


@router.post(Constants.PASSWORD_RESET_PREFIX)
async def wants_reset_password(
    data: PasswordWantsResetSchema,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    authenticator = Depends(
        auth_service,
    ),
):
    try:
        authenticator.wants_reset_password(
            data,
            client,
        )
    except Exception as e:
        log.exception(e)

        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.put(Constants.PASSWORD_RESET_PREFIX)
async def will_reset_password(
    data: PasswordWillResetSchema,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    authenticator = Depends(
        auth_service,
    ),
):
    try:
        authenticator.will_reset_password(
            data,
            client,
        )
    except Exception as e:
        log.exception(e)
        
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    return Response(
        status_code=HTTP_200_OK,
    )


@router.post(Constants.SIGNIN_PREFIX)
async def sign_in(
    data: UserLoginSchema,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    authenticator = Depends(
        auth_service,
    ),
):
    try:
        return authenticator.sign_in(
            data,
            client,
        )
    except ValueError as e:
        log.exception(e)
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        log.exception(e)
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post(Constants.LOGOUT_PREFIX)
async def sign_out(
    authenticator = Depends(
        auth_service,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    refresh_token: Optional[str] = Cookie(None),
    ssm_service: AWSService.SSMService = Depends(
        ssm_service,
    ),
):
    log.info(
        'Signing out...',
    )

    _, email, _ = authenticator.is_valid_token(refresh_token)

    with client.start_session(causal_consistency=True) as session:
        collection = client.visibly.users
        collection.update_one(
            { 'email': email },
            { '$set': { 'refresh_token': str(uuid.uuid4()) } },
        )

    response = JSONResponse(
        content={
            'message': Constants.USER_SIGNED_OUT,
        }
    )
    response.delete_cookie(
        domain=ssm_service.cookie_domain,
        key=Constants.REFRESH_TOKEN_COOKIE_KEY,
    )

    log.info('Signed out')

    return response
