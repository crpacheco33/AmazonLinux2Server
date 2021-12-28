from typing import (
    Any,
    List,
    Optional,
    Union,
)

from bson import json_util
from bson.objectid import ObjectId
from pydantic import (
    BaseModel,
    validator,
)

import datetime
import enum
import re

from server.core.constants import Constants
from server.resources.schema.base import BaseModelMixin
from server.resources.types.data_types import (
    ReportStateType,
    ReportTemplateType,
)


class ObjectIdSchema(BaseModelMixin):
    pass


class ReportCreateSchema(BaseModel):
    end_date: str
    insights: List[str]
    start_date: str
    tags: Optional[List[str]]
    template: ReportTemplateType
    title: str

    @validator(Constants.END_DATE)
    def transform_end_date(cls, value):
        if re.match(r'^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|1[0-9]|2[0-9]|3[01])$', value) is None:
            raise ValueError(
                'Invalid date format. Required:[YYYY-MM-DD].'
            )
        return value
        
    @validator(Constants.START_DATE)
    def transform_start_date(cls, value, values, **kwargs):
        if 'end_date' in values and value > values['end_date']:
            raise ValueError(
                'Invalid date format. Required:[YYYY-MM-DD]. Start date must be before end date. '
            )
        if re.match(r'^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|1[0-9]|2[0-9]|3[01])$', value) is None:
            raise ValueError(
                'Invalid date format. Required:[YYYY-MM-DD].'
            )
        return value


class ReportIndexSchema(BaseModelMixin):
    title: str
    insights: List[str]
    template: ReportTemplateType
    state: ReportStateType
    end_date: datetime.date
    start_date: datetime.date
    tags: Optional[List[str]]
    created_at: Optional[str] = None

    @validator(
        Constants.CREATED_AT,
        always=True,
    )
    def transform_datetimes(cls, value, values):
        return values['id'].generation_time.strftime(
            Constants.REPORT_CREATED_AT_DATETIME_FORMAT
        )

    @validator(
        Constants.INSIGHTS,
        Constants.TAGS,
        pre=True,
        allow_reuse=True,
        each_item=True,
    )
    def transform_tag(cls, value):
        return str(value)