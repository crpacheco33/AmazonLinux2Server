from typing import (
    Any,
    List,
    Optional,
    Union,
)

import enum
import re

from pydantic import (
    BaseModel,
    confloat,
    Field,
    validator,
)

from server.core.constants import Constants
from server.resources.types.data_types import (
    BrandAnalyticsIntervalType,
    BudgetType,
    ExpressionType,
    StateType,
)
from server.utilities.data_utility import DataUtility


class APIIndexSchema(BaseModel):
    data: List[Any]
    total_count: int

    class Config:
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True


class DataSchema(BaseModel):
    total_spend: Optional[float]
    total_clicks: Optional[float]
    total_sales: Optional[float]
    total_impressions: Optional[float]
    ctr: Optional[float]


class ShowDSPCreativeSchema(BaseModel):
    creative_id: str
    external_id: Optional[str] = None
    name: str
    size: Optional[str] = None
    type: Optional[str] = None

    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True


class ShowDSPLineItemSchema(BaseModel):
    line_item_id: str
    order_id: str
    comments: Optional[str] = None
    creation_date: Optional[str] = None
    currency_code: Optional[Union[str, enum.Enum]] = None
    delivery_status: Optional[Union[str, enum.Enum]] = None
    end_date_time: Optional[str] = None
    external_id: Optional[str] = None
    last_update_date: Optional[str] = None
    line_item_type: Optional[Union[str, enum.Enum]] = None
    name: str
    start_date_time: Optional[str] = None

    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True

    @validator(
        'currency_code',
        'delivery_status',
        'line_item_type',
        allow_reuse=True,
        each_item=True,
        pre=True,
    )
    def transform_enum(cls, value):
        try:
            return value.name
        except AttributeError:
            return value


class ShowDSPOrderSchema(BaseModel):
    order_id: str
    name: str
    comments: Optional[str] = None
    creation_date: Optional[str] = None
    currency_code: Optional[Union[str, enum.Enum]] = None
    delivery_status: Optional[Union[str, enum.Enum]] = None
    end_date_time: Optional[str] = None
    external_id: Optional[str] = None
    last_update_date: Optional[str] = None
    start_date_time: Optional[str] = None

    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True

    @validator(
        'currency_code',
        'delivery_status',
        allow_reuse=True,
        each_item=True,
        pre=True,
    )
    def transform_enum(cls, value):
        try:
            return value.name
        except AttributeError:
            return value


class ShowPortfolioSchema(BaseModel):
    creation_date: Optional[str] = None
    in_budget: Optional[bool] = None
    last_updated_date: Optional[str] = None
    name: str
    portfolio_id: str
    serving_status: Optional[Union[str, enum.Enum]] = None
    state: Optional[Union[str, enum.Enum]] = None

    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True

    @validator(
        'serving_status',
        'state',
        allow_reuse=True,
        each_item=True,
        pre=True,
    )
    def transform_enums(cls, value):
        try:
            return value.name
        except AttributeError:
            return value


class ShowSBAdGroupSchema(BaseModel):
    bid: float
    state: Optional[Union[str, enum.Enum]] = None
    name: str
    ad_group_id: str
    campaign_id: str

    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True

    @validator('state', pre=True, allow_reuse=True, each_item=True)
    def transform_enum(cls, value):
        try:
            return value.name
        except AttributeError:
            return value


class ShowSBCampaignSchema(DataSchema):
    campaign_id: str
    ad_format: Union[str, enum.Enum]
    bid_multiplier: Optional[int] = None
    budget: float
    budget_type: Union[str, enum.Enum]
    campaign_type: str = Constants.SPONSORED_BRANDS
    creation_date: Optional[int] = None
    delivery_profile: Optional[str] = None
    end_date: Optional[str] = None
    last_updated_date: Optional[str] = None
    name: str
    placement: Optional[str] = None
    portfolio_id: Optional[str] = None
    premium_bid_adjustment: Optional[bool] = None
    schedule: Optional[str] = None
    serving_status: Optional[Union[str, enum.Enum]]
    start_date: str
    state: Union[str, enum.Enum]

    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True

    @validator(
        'ad_format',
        'budget_type',
        # 'serving_status',
        'state',
        allow_reuse=True,
        each_item=True,
        pre=True,
    )
    def transform_enum(cls, value):
        try:
            return value.name
        except AttributeError:
            return value


class ShowSBKeywordSchema(BaseModel):
    bid: float
    keyword_text: str
    match_type: Union[str, enum.Enum]
    serving_status: Optional[Union[str, enum.Enum]] = None
    state: Union[str, enum.Enum]
    ad_group_id: str
    campaign_id: str
    keyword_id: str

    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True

    @validator(
        'match_type',
        'serving_status',
        'state',
        allow_reuse=True,
        each_item=True,
        pre=True,
    )
    def transform_enum(cls, value):
        try:
            return value.name
        except AttributeError:
            return value


class ShowSBTargetSchema(BaseModel):
    bid: float
    state: Union[str, enum.Enum]
    ad_group_id: str
    campaign_id: str
    target_id: str

    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True

    @validator('state', pre=True, allow_reuse=True, each_item=True)
    def transform_enum(cls, value):
        try:
            return value.name
        except AttributeError:
            return value


class ShowSDAdGroupSchema(BaseModel):
    default_bid: float
    creation_date: int
    last_updated_date: Optional[str] = None
    name: str
    serving_status: Union[str, enum.Enum]
    state: Optional[Union[str, enum.Enum]] = None
    ad_group_id: str
    campaign_id: str

    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True

    @validator(
        'serving_status',
        'state',
        allow_reuse=True,
        each_item=True,
        pre=True,
    )
    def transform_enum(cls, value):
        try:
            return value.name
        except AttributeError:
            return value


class ShowSDCampaignSchema(BaseModel):
    campaign_id: str
    budget: float
    budget_type: Union[str, enum.Enum]
    cost_type: str
    creation_date: Optional[int] = None
    delivery_profile: str = None
    end_date: Optional[str] = None
    last_updated_date: Optional[str] = None
    name: str
    portfolio_id: Optional[str] = None
    schedule: Optional[str] = None
    serving_status: Optional[Union[str, enum.Enum]]
    start_date: str
    state: Union[str, enum.Enum]
    tactic: Union[str, enum.Enum]

    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True

    @validator(
        'budget_type',
        'serving_status',
        'state',
        'tactic',
        allow_reuse=True,
        each_item=True,
        pre=True,
    )
    def transform_enum(cls, value):
        try:
            return value.name
        except AttributeError:
            return value


class ShowSDProductAdSchema(BaseModel):
    campaign_id: str
    ad_group_id: str
    ad_id: str
    asin: str
    creation_date: int
    sku: Optional[str] = None
    last_updated_date: Optional[str] = None
    serving_status:  Union[str, enum.Enum]
    state: Union[str, enum.Enum]

    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True

    @validator(
        'serving_status',
        'state',
        allow_reuse=True,
        pre=True,
        each_item=True,
    )
    def transform_enum(cls, value):
        try:
            return value.name
        except AttributeError:
            return value


class ShowSDTargetSchema(BaseModel):
    ad_group_id: str
    target_id: str
    bid: float
    creation_date: int
    expression_type: Optional[Union[str, enum.Enum]] = Field(
        None,
        alias='expressionType'
    )
    last_updated_date: Optional[str] = None
    serving_status: Union[str, enum.Enum]
    state: Union[str, enum.Enum]

    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True

    @validator(
        'state',
        'serving_status',
        'expression_type',
        allow_reuse=True,
        each_item=True,
        pre=True,
    )
    def transform_enum(cls, value):
        try:
            return value.name
        except AttributeError:
            return value


class ShowSPAdGroupSchema(BaseModel):
    ad_group_id: str
    campaign_id: str
    name: str
    creation_date: int
    default_bid: float
    last_updated_date: Optional[str] = None
    serving_status:  Union[str, enum.Enum]
    state: Union[str, enum.Enum]

    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True

    @validator(
        'serving_status',
        'state',
        allow_reuse=True,
        each_item=True,
        pre=True,
    )
    def transform_enum(cls, value):
        try:
            return value.name
        except AttributeError:
            return value


class ShowSPCampaignSchema(BaseModel):
    campaign_id: str
    campaign_type: str = 'sponsoredProducts'
    creation_date: Optional[int]
    daily_budget: float
    end_date: Optional[str] = None
    last_updated_date: Optional[str] = None
    name: str
    placement: Optional[str] = None
    portfolio_id: Optional[str] = None
    premium_bid_adjustment: bool
    schedule: Optional[str] = None
    serving_status:  Optional[Union[str, enum.Enum]]
    start_date: str
    state: Union[str, enum.Enum]
    targeting_type: Union[str, enum.Enum]

    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True

    @validator(
        'campaign_type',
        'serving_status',
        'state',
        'targeting_type',
        allow_reuse=True,
        each_item=True,
        pre=True,
    )
    def transform_enum(cls, value):
        try:
            return value.name
        except AttributeError:
            return value


class ShowSPProductAdSchema(BaseModel):
    ad_group_id: str
    ad_id: str
    campaign_id: str
    asin: Optional[str] = None
    creation_date: int
    last_updated_date: Optional[str] = None
    serving_status:  Union[str, enum.Enum]
    sku: Optional[str] = None
    state: Union[str, enum.Enum]

    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True

    @validator(
        'serving_status',
        'state',
        allow_reuse=True,
        each_item=True,
        pre=True,
    )
    def transform_enum(cls, value):
        try:
            return value.name
        except AttributeError:
            return value


class ShowSPKeywordSchema(BaseModel):
    ad_group_id: str
    campaign_id: str
    keyword_id: str
    bid: float
    keyword_text: str
    match_type:  Union[str, enum.Enum]
    serving_status:  Union[str, enum.Enum]
    state: Union[str, enum.Enum]

    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True

    @ validator(
        'match_type',
        'serving_status',
        'state',
        pre=True,
        allow_reuse=True,
        each_item=True,
    )
    def transform_enum(cls, value):
        try:
            return value.name
        except AttributeError:
            return value


class ShowSPTargetSchema(BaseModel):
    ad_group_id: str
    campaign_id: str
    target_id: str
    bid: float
    expression_type: Optional[Union[str, enum.Enum]] = Field(
        None,
    )
    state: Union[str, enum.Enum]
    
    class Config:
        orm_mode = True
        alias_generator = DataUtility().to_camel_case
        allow_population_by_field_name = True

    @validator(
        'state',
        'expression_type',
        allow_reuse=True,
        each_item=True,
        pre=True,
    )
    def transform_enum(cls, value):
        try:
            return value.name
        except AttributeError:
            return value

class IndexDSPCreativeSchema(APIIndexSchema):
    data: List[ShowDSPCreativeSchema]


class IndexDSPLineItemSchema(APIIndexSchema):
    data: List[ShowDSPLineItemSchema]


class IndexDSPOrderSchema(APIIndexSchema):
    data: List[ShowDSPOrderSchema]


class IndexSBAdGroupSchema(APIIndexSchema):
    data: List[ShowSBAdGroupSchema]


class IndexSBCampaignSchema(APIIndexSchema):
    data: List[ShowSBCampaignSchema]


class IndexSBKeywordSchema(APIIndexSchema):
    data: List[ShowSBKeywordSchema]


class IndexSBTargetSchema(APIIndexSchema):
    data: List[ShowSBTargetSchema]


class IndexSDAdGroupSchema(APIIndexSchema):
    data: List[ShowSDAdGroupSchema]


class IndexSDCampaignSchema(APIIndexSchema):
    data: List[ShowSDCampaignSchema]


class IndexSDProductAdSchema(APIIndexSchema):
    data: List[ShowSDProductAdSchema]


class IndexSDTargetSchema(APIIndexSchema):
    data: List[ShowSDTargetSchema]


class IndexSearchTermsRankingSchema(BaseModel):
    search_terms: List[str]
    start_date: str  # YYYY-MM-DD
    end_date: str  # YYYY-MM-DD
    interval: BrandAnalyticsIntervalType = Field(
        BrandAnalyticsIntervalType.WEEKLY
    )

    @validator('start_date')
    def validate_start_date(cls, value):
        if re.match(r'^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|1[0-9]|2[0-9]|3[01])$', value) is None:
            raise ValueError(
                'Search Terms start date is not using the correct format (YYYY-MM-DD)',
            )
        return value

    @validator('end_date')
    def validate_end_date(cls, value):
        if re.match(r'^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|1[0-9]|2[0-9]|3[01])$', value) is None:
            raise ValueError(
                'Search Terms start date is not using the correct format (YYYY-MM-DD)',
            )
        return value


class IndexSearchTermsResponseSchema(BaseModel):
    rank: int = Field(None, alias='search_frequency_rank')
    search_term: str
    first_clicked_asin: str
    first_product_title: str
    first_click_share: float
    first_conversion_share: float
    second_clicked_asin: str
    second_product_title: str
    second_click_share: float
    second_conversion_share: float
    third_clicked_asin: str
    third_product_title: str
    third_click_share: float
    third_conversion_share: float

    class Config:
        validation = True


class IndexSearchTermsSchema(BaseModel):
    asins: List[str]
    exclude: List[str]
    product_titles: List[str]
    report_range: str
    search_terms: List[str]

    @validator('report_range')
    def validate_report_range(cls, value):
        if re.match(r'^(0?[1-9]|1[012])\-\d{4}$', value) is None:
            raise ValueError(
                'Search Terms report range is not using the correct format (MM-YYYY).',
            )
        return value


class IndexPortfolioSchema(APIIndexSchema):
    data: List[Any]


class IndexSPAdGroupSchema(APIIndexSchema):
    data: List[ShowSPAdGroupSchema]


class IndexSPCampaignSchema(APIIndexSchema):
    data: List[ShowSPCampaignSchema]


class IndexSPProductAdSchema(APIIndexSchema):
    data: List[ShowSPProductAdSchema]


class IndexSPKeywordSchema(APIIndexSchema):
    data: List[ShowSPKeywordSchema]


class IndexSPTargetSchema(APIIndexSchema):
    data: List[ShowSPTargetSchema]


class UpdateDSPCreativeSchema(BaseModel):
    pass


class UpdateDSPLineItemSchema(BaseModel):
    pass


class UpdateSBAdGroupSchema(BaseModel):
    name: Optional[str]
    adGroupId: Optional[int]
    campaignId: Optional[int]


class UpdateSBCampaignSchema(BaseModel):
    bidMultiplier: Optional[confloat(ge=-99.0, le=99.0)]
    bidOptimization: Optional[bool]
    budget: Optional[float]
    endDate: Optional[str]
    name: Optional[str]
    state: Union[str, enum.Enum] = None
    tags: Optional[Any] = None
    campaignId: Optional[int]
    portfolioId: Optional[int]


class UpdateSBKeywordSchema(BaseModel):
    bid: Optional[float] = None
    state: Union[str, enum.Enum] = None
    adGroupId: Optional[int]
    campaignId: Optional[int]
    keywordId: Optional[int]


class UpdateSBTargetItemSchema(BaseModel):
    bid: Optional[float] = None
    state: Union[str, enum.Enum] = None
    adGroupId: Optional[int]
    targetId: Optional[int]


class UpdateSBTargetSchema(BaseModel):
    targets: List[UpdateSBTargetItemSchema]


class UpdateSDAdGroupSchema(BaseModel):
    adGroupId: int
    campaignId: int
    defaultBid: Optional[float] = None
    name: str
    state: Union[str, enum.Enum] = None


class UpdateSDCampaignSchema(BaseModel):
    campaignId: Optional[int]
    budget: Optional[float]
    budgetType: Optional[BudgetType]
    endDate: Optional[str]
    name: Optional[str] 
    startDate: Optional[str]
    state: Optional[StateType]
    tactic: Optional[str]
    tags: Optional[Any] = None


class UpdateSDProductAdSchema(BaseModel):
    adId: Optional[int]
    adGroupId: Optional[int]
    campaignId: Optional[int]
    asin: Optional[str] = None
    sku: Optional[str] = None
    state: Optional[StateType] = None


class TargetingExpressionSchema(BaseModel):
    __root__: List[Any] = Field(...)


class UpdateSDTargetSchema(BaseModel):
    adGroupId: Optional[int] = None
    targetId: int
    bid: Optional[float] = None
    expression: Optional[TargetingExpressionSchema]
    expressionType: Optional[ExpressionType] = None
    state: Optional[StateType] = None


class UpdateSPAdGroupSchema(BaseModel):
    adGroupId: Optional[int]
    defaultBid: Optional[float]
    name: Optional[str]
    state: Optional[StateType] = None


class UpdateSPCampaignSchema(BaseModel):
    bidding: Optional[Any] = None
    campaignId: Optional[int]
    portfolioId: Optional[Any]
    dailyBudget: Optional[confloat(ge=1.0)]
    endDate: Optional[str]
    name: Optional[str]
    premiumBidAdjustment: Optional[bool]
    startDate: Optional[str]
    state: Optional[StateType] = None
    tags: Optional[Any] = None


class UpdateSPKeywordSchema(BaseModel):
    keywordId: Optional[int]
    bid: Optional[confloat(ge=0.02)]
    state: Optional[StateType] = None


class UpdateSPProductAdSchema(BaseModel):
    adId: Optional[int]
    state: Optional[StateType] = None


class SPTargetingExpressionPredicate(BaseModel):
    type: Optional[ExpressionType] = None
    value: Optional[str] = None


class UpdateSPTargetSchema(BaseModel):
    targetId: Optional[int]
    bid: Optional[float] = None
    expression: Optional[List[SPTargetingExpressionPredicate]]
    expressionType: Optional[ExpressionType] = None
    resolvedExpression: Optional[List[SPTargetingExpressionPredicate]]
    state: Optional[StateType] = None
