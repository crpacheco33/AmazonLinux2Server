from typing import Optional

from bson import json_util
from bson.objectid import ObjectId
from pydantic import (
    BaseModel,
    Field,
    validator,
)


class PyObjectId(ObjectId):
    """Represents DocumentDB's ObjectId."""
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError('Invalid objectid')
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type='string')


class BaseModelMixin(BaseModel):
    """Modifies pydantic's `BaseModel` to support DocumentDB identifiers.
    
    `BaeModelMixin` aliases DocumentDB's `_id` as `id`.
    """
    id: Optional[PyObjectId] = Field(..., alias='_id')

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_dumps = json_util.dumps
        json_encoders = {ObjectId: lambda x: str(x)}
        json_loads = json_util.loads