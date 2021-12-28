from datetime import datetime

from typing import (
    Any,
    List,
    Optional,
)

from bson import json_util
from bson.objectid import ObjectId
from pydantic import (
    BaseModel,
    Field,
    StrictStr,
    validator,
)

from server.core.constants import Constants
from server.resources.schema.base import BaseModelMixin
from server.resources.types.data_types import AdType


class TagCampaignSchema(BaseModel):
    ad_type: AdType
    campaignId: str


class TagCreateCampaignSchema(BaseModel):
    ad_type: str
    campaignId: str


class TagCreateSchema(BaseModel):
    name: str
    prefix: Optional[str]
    campaigns: Optional[List[TagCreateCampaignSchema]] = []
    insights: Optional[List[str]] = []
    orders: Optional[List[str]] = []


class TagIndexSchema(BaseModelMixin):
    count: int = None
    name: str
    prefix: Optional[str] = Constants.EMPTY_STRING
    campaigns: Optional[List[TagCampaignSchema]]
    insights: Optional[List[str]]
    orders: Optional[List[str]]

    @validator(
        Constants.INSIGHTS,
        pre=True,
        allow_reuse=True,
        each_item=True,
    )
    def transform_tag(cls, value):
        return str(value)


class TagNewCampaignSchema(BaseModel):
    ad_type: str = Field(..., alias='_path')
    campaignId: str
    name: str
    startDate: str
    endDate: Optional[str]

    @validator('ad_type')
    def transform_path(cls, value):
        if AdType.SB in value:
            return AdType.SB
        elif AdType.SD in value:
            return AdType.SD
        elif AdType.SP in value:
            return AdType.SP

    class Config:
        allow_population_by_field_name = True


class TagNewOrderSchema(BaseModel):
    orderId: str
    name: str
    startDateTime: str = Field(..., alias='startDate')
    endDateTime: str = Field(..., alias='endDate')

    @validator('startDateTime', 'endDateTime')
    def transform_start_date_time(cls, value):
        date = datetime.strptime(
            value,
            Constants.DATE_FORMAT_DSP,
        )
        return datetime.strftime(
            date,
            Constants.DATE_FORMAT_YYYYMMDD,
        )

    class Config:
        allow_population_by_field_name = True


class TagSchema(BaseModelMixin):
    name: str
    prefix: str
    campaigns: Optional[List[TagCreateCampaignSchema]] = []
    insights: Optional[List[str]] = []
    orders: Optional[List[str]] = []

    @validator(
        Constants.INSIGHTS,
        pre=True,
        allow_reuse=True,
        each_item=True,
    )
    def transform_tag(cls, value):
        return str(value)
    
    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class TagEditSchema(BaseModel):
    tag: TagSchema
    campaigns: List[TagNewCampaignSchema]
    orders: List[TagNewOrderSchema]

    class Config:
        json_loads = json_util.loads
        json_dumps = json_util.dumps
        allow_population_by_field_name = True
        json_encoders = {ObjectId: lambda x: str(x)}


class TagNewSchema(BaseModel):
    campaigns: List[TagNewCampaignSchema]
    orders: List[TagNewOrderSchema]


class TagUpdateSchema(BaseModel):
    id: str
    name: str
    prefix: str
    insights: Optional[List[str]] = []
    campaigns: Optional[List[dict]] = []
    orders: Optional[List[str]] = []
