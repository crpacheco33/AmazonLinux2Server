from typing import (
    List,
    Optional,
    Union,
)

import enum
import re

from pydantic import (
    BaseModel,
    constr,
    Field,
    validator,
)

from server.core.constants import Constants
from server.resources.schema.base import BaseModelMixin
from server.resources.types.data_types import (
    AdType,
    BidType,
    PlatformType,
    RegionType,
)


class BidType(str, enum.Enum):
    ADJUSTED='adjusted'
    DYNAMIC='dynamic'
    ORIGINAL='original'


class BidActionType(str, enum.Enum):
    DECREASE='decrease'
    INCREASE='increase'


class DynamicType(str, enum.Enum):
    AMOUNT='amount'
    PERCENT='percent'


class BidAdjustmentSchema(BaseModelMixin):
    action: Optional[BidActionType]
    dynamic_type: Optional[DynamicType]
    bid_type: BidType
    limit: Optional[float]
    value: Optional[str] = None

    class Config:
        use_enum_values = True


class CreateUpdateDaypartSchema(BaseModel):
    ad_type: AdType
    bids: Optional[List[Union[BidAdjustmentSchema, dict]]]
    campaign_id: str
    platform: PlatformType
    schedule: Optional[constr(regex=Constants.DAYPART_SCHEDULE_PATTERN)] = None

    class Config:
        use_enum_values = True

class DaypartSchema(BaseModelMixin):
    ad_type: AdType
    advertiser_id: str
    bids: Optional[List[BidAdjustmentSchema]]
    campaign_id: str
    platform: PlatformType
    region: RegionType
    schedule: constr(regex=Constants.DAYPART_SCHEDULE_PATTERN)

    class Config:
        use_enum_values = True
