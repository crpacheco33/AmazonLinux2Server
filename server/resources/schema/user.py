from typing import (
    List,
    Optional,
)

import re

from pydantic import (
    BaseModel,
    EmailStr,
    validator,
)

from server.core.constants import Constants
from server.resources.types.data_types import (
    EmailType,
    ScopeType,
    UserStatusType,
)


class PasswordWantsResetSchema(BaseModel):
    email: EmailStr


class PasswordWillResetSchema(BaseModel):
    code: str
    password: str
    sid: str

    @validator(Constants.PASSWORD)
    def validate_password(cls, value):
        if len(str(value)) < 8 or len(str(value)) > 32:
            raise ValueError(
                'Passwords should be between 8 and 32 characters long.')

        if not re.match(Constants.PASSWORD_PATTERN, str(value)):
            raise ValueError(
                'Passwords must contain ASCII letters only and at least one each of digits, uppercase and lowercase letters, and symbols []()-+!.-="<>@~.'
            )

        return value


class ResendEmailSchema(BaseModel):
    email: EmailStr
    email_type: EmailType


class User2FASchema(BaseModel):
    code: str
    sid: str


class UserAuthSchema(BaseModel):
    id: str
    email: str
    password: str
    full_name: Optional[str] = None

    status: UserStatusType
    scopes: List[ScopeType]

    class Config:
        orm_mode = True


class UserInvitationSchema(BaseModel):
    hash: str
    password: str


class UserLoginSchema(BaseModel):
    email: EmailStr
    password: str


class UserPasswordUpdateSchema(BaseModel):
    new_password: str
    password: str

    @validator(Constants.NEW_PASSWORD)
    def validate_password(cls, value):
        if len(str(value)) < 8 or len(str(value)) > 32:
            raise ValueError(
                'Passwords should be between 8 and 32 characters long.')

        if not re.match(Constants.PASSWORD_PATTERN, str(value)):
            raise ValueError(
                'Passwords must contain letters from the alphabet, at least one digit, uppercase and lowercase letter, and a symbol []()-+!.-="<>@~.'
            )

        return value