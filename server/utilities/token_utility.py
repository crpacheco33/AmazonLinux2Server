from bson.objectid import ObjectId
from fastapi.params import Depends
from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBearer,
)
from jose import (
    JWTError,
    jwt,
)
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.status import HTTP_401_UNAUTHORIZED

from server.core.constants import Constants
from server.resources.models.brand import Brand
from server.resources.schema.token import (
    AccessToken,
    Header,
    RefreshToken,
    Token,
)
from server.services.auth_service import AuthService
from server.services.aws_service import AWSService


log = AWSService().log_service

class Bearer(HTTPBearer):

    def __init__(self):
        super().__init__(
            auto_error=True,
        )

    async def __call__(self, request: Request):
        credentials: HTTPAuthorizationCredentials = await super().__call__(request)
        aws_service = AWSService()
        ssm_service = aws_service.ssm_service
        authenticator = AuthService(
            ssm_service,
            None,
        )

        if credentials:
            if not credentials.scheme == Constants.BEARER:
                raise HTTPException(
                    status_code=HTTP_401_UNAUTHORIZED,
                    detail=Constants.MISSING_BEARER_AUTHENTICATION,
                )
        
            jwt_token = credentials.credentials

            try:
                message, signature = jwt_token.rsplit(
                    Constants.PERIOD,
                    1,
                )
            except ValueError:
                raise HTTPException(
                    status_code=HTTP_401_UNAUTHORIZED,
                    detail=Constants.INVALID_TOKEN_FORMAT,
                )
            
            try:
                client = aws_service.docdb_service.client

                token = Token(
                    claims=AccessToken.parse_obj(
                        jwt.get_unverified_claims(jwt_token),
                    ),
                    header=Header.parse_obj(
                        jwt.get_unverified_header(jwt_token),
                    ),
                    jwt_token=jwt_token,
                )

                brands = Brand.with_user(
                    ObjectId(token.claims.id),
                    client,
                )

                brand_ids = [str(brand._id) for brand in brands]

                if not brand_ids or not token.claims.brand in brand_ids:
                    raise HTTPException(
                        status_code=HTTP_401_UNAUTHORIZED,
                        detail=Constants.USER_ACCESS_DENIED,
                    )

                if not authenticator.is_valid_token(token.jwt_token):
                    raise HTTPException(
                        status_code=HTTP_401_UNAUTHORIZED,
                        detail=Constants.INVALID_JWT_TOKEN,
                    )
            except JWTError as e:
                log.exception(e)
                raise HTTPException(
                    status_code=HTTP_401_UNAUTHORIZED,
                    detail=Constants.SESSION_EXPIRED,
                )

            return token
