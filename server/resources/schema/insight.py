from typing import (
    Any,
    Dict,
    List,
    Optional,
    Union,
)

import datetime
import enum

from pydantic import (
    BaseModel,
    Field,
    validator,
)

from server.core.constants import Constants
from server.resources.types.data_types import (
    InsightMetaStateType,
    InsightStateType,
    InsightType,
)
from server.resources.schema.base import BaseModelMixin
from server.resources.schema.tag import TagIndexSchema


class AdType(str, enum.Enum):
    SB='sb'
    SD='sd'
    SP='sp'


class InsightMetaSchema(BaseModel):
    action: str
    ad_type: AdType
    advertiser_id: str
    campaign_id: str
    insight_type: str
    region: str
    state: Optional[InsightMetaStateType]
    value: float


class InsightCreateSchema(BaseModel):
    action: str
    date: str
    description: str
    tags: Optional[List[str]]
    title: str

    @validator(
        Constants.DATE,
        pre=True,
    )
    def transform_date(cls, value):
        try:
            return datetime.datetime.strftime(
                datetime.datetime.strptime(
                    value,
                    Constants.DATE_FORMAT_MMDDYYYY,
                ),
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )
        except ValueError as e:
            print(e)
            pass
        
        return value

    @validator(
        Constants.TAGS,
        pre=True,
        allow_reuse=True,
        each_item=True,
    )
    def transform_tag(cls, value):
        return str(value)


class InsightIndexTagNameSchema(BaseModelMixin):
    action: str
    date: datetime.date
    description: str
    meta: Optional[InsightMetaSchema] = None
    state: Optional[InsightStateType]
    tags: List[dict]
    title: str
    type: InsightType


class InsightIndexSchema(BaseModelMixin):
    action: str
    date: datetime.date
    description: str
    meta: Optional[InsightMetaSchema] = None
    state: Optional[InsightStateType]
    tags: List[str]
    title: str
    type: InsightType

    @validator(
        Constants.TAGS,
        pre=True,
        allow_reuse=True,
        each_item=True,
    )
    def transform_tag(cls, value):
        return str(value)


class InsightShowSchema(BaseModel):
    action: str
    date: datetime.date
    description: str
    meta: Optional[InsightMetaSchema] = None
    state: Optional[InsightStateType]
    tags: List[dict]
    title: str
    type: InsightType


class InsightUpdateSchema(BaseModel):
    action: str
    date: str
    description: str
    meta: Optional[InsightMetaSchema] = None
    state: Optional[InsightStateType]
    tags: Optional[List[str]]
    title: str

    @validator(Constants.DATE)
    def transform_date(cls, value):
        try:
            return datetime.datetime.strftime(
                datetime.datetime.strptime(
                    value,
                    Constants.DATE_FORMAT_MMDDYYYY,
                ),
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )
        except ValueError as e:
            print(e)
            pass
        
        return value

    @validator(
        Constants.TAGS,
        pre=True,
        allow_reuse=True,
        each_item=True,
    )
    def transform_tag(cls, value):
        return str(value)
