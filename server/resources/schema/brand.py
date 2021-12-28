from typing import (
    List,
    Optional,
    Union,
)

from bson import json_util
from bson.objectid import ObjectId
from pydantic import (
    BaseModel,
    Field,
)

from server.resources.schema.base import (
    BaseModelMixin,
    PyObjectId,
)


class BrandAmazonSchema(BaseModel):
    aa: dict
    sp: Optional[dict] = None


class BrandIndexSchema(BaseModelMixin):
    country: str
    name: str
    timezone: str
    

class BrandSchema(BaseModel):
    accounts: List[dict]
    dayparts: List[str]
    insights: List[str]
    reports: List[str]
    tags: List[str]
    users: List[str]


class BrandShowSchema(BaseModelMixin):
    name: str
    currency: str
    timezone: str
    amazon: BrandAmazonSchema
