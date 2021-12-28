from typing import (
    List,
    Optional,
)

from pydantic import BaseModel

from server.core.constants import Constants
from server.resources.types.data_types import ScopeType


class AccessToken(BaseModel):
    id: str
    brand: str
    email: str
    exp: Optional[int]
    full_name: Optional[str]
    iss: str = Constants.VISIBLY
    scopes: List[ScopeType]


class Header(BaseModel):
    alg: str
    typ: str


class RefreshToken(BaseModel):
    brand_id: str
    email: str
    exp: Optional[str]
    version: str


class Token(BaseModel):
    header: Header
    claims: AccessToken
    jwt_token: str
    refresh_token: Optional[str]