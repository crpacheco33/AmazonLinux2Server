from typing import (
    List,
)

import enum

from pydantic import (
    BaseModel,
    EmailStr,
    validator,
)

from server.resources.types.data_types import ScopeType


class UserOnboardSchema(BaseModel):
    email: EmailStr
    name: str
    brands: List[str]
    scopes: List[ScopeType]


class UsersOnboardSchema(BaseModel):
    __root__: List[UserOnboardSchema]