"""Type information used by `server`.

Types are expressed as `enum`s and type values should match the names as
described by external APIs where applicable. In other cases, type values
should be expressed using snake case.
"""


import enum

from server.core.constants import Constants


class AdType(str, enum.Enum):
    SB='sb'
    SD='sd'
    SP='sp'


class ApiType(str, enum.Enum):
    DSP='dsp'
    SA='sa'


class BidType(str, enum.Enum):
    ABSOLUTE_VALUE='absolute'
    PERCENTAGE_VALUE='percentage'


class BrandAnalyticsDistributorType(str, enum.Enum):
    MANUFACTURING='manufacturing'
    SOURCING='sourcing'


class BrandAnalyticsIntervalType(str, enum.Enum):
    DAILY='DAILY'
    MONTHLY='MONTHLY'
    QUARTERLY='QUARTERLY'
    WEEKLY='WEEKLY'
    YEARLY='YEARLY'


class BrandAnalyticsReportType(str, enum.Enum):
    DEMAND_FORECAST='demand-forecast'
    INVENTORY_HEALTH_AND_PLANNING='inventory-health-and-planning'
    SALES_DIAGNOSTIC='sales-diagnostic'


class BrandAnalyticsSalesType(str, enum.Enum):
    ORDERED_REVENUE='orderedRevenue'
    SHIPPED_COGS='shippedCOGS'
    SHIPPED_REVENUE='shippedRevenue'


class BrandAnalyticsSellingProgramType(str, enum.Enum):
    AMAZON_FRESH='AMAZON_FRESH'
    AMAZON_PRIME_NOW='AMAZON_PRIME_NOW'
    AMAZON_RETAIL='AMAZON_RETAIL'


class BudgetType(str, enum.Enum):
    DAILY='daily'
    

class CreativeType(str, enum.Enum):
    PRODUCT_COLLECTION='productCollection'
    VIDEO='video'


class EmailType(str, enum.Enum):
    CONFIRM='confirm'
    INVITE='invite'
    RESET='reset'
    

class ExpressionType(str, enum.Enum):
    AUTO='auto'
    MANUAL='manual'


class FunnelType(str, enum.Enum):
    AW='AW'
    CS='CS'
    CV='CV'

    @classmethod
    def to_string_list(cls):
        return Constants.COMMA.join([segment.value for segment in cls])


class IntervalType(str, enum.Enum):
    DAY='day'
    MONTH='month'
    QUARTER='quarter'
    WEEK='week'
    YEAR='year'


class InsightMetaStateType(str, enum.Enum):
    ACCEPTED='accepted'
    DISMISSED='dismissed'


class InsightMetaType(str, enum.Enum):
    BUDGET_RECOMMENDATION='budget_recommendation'


class InsightStateType(str, enum.Enum):
    FAVORITE='favorite'


class InsightType(str, enum.Enum):
    AUTOMATED='automated'
    MANUAL='manual'


class KeywordMatchType(str, enum.Enum):
    BROAD='broad'
    EXACT='exact'
    PHRASE='phrase'


class ObjectiveType(str, enum.Enum):
    AM='AM'
    BP='BP'
    CQ='CQ'
    DC='DC'
    UZ='UZ'
    XM='XM'

    @classmethod
    def to_string_list(cls):
        return Constants.COMMA.join([objective.value for objective in cls])


class PlatformType(str, enum.Enum):
    AA='aa'


class RegionType(str, enum.Enum):
    NA='na'
    EU='eu'
    FE='fe'


class ReportStateType(str, enum.Enum):
    ERROR='error'
    PENDING='pending'
    READY='ready'


class ReportTemplateType(str, enum.Enum):
    DSP_OVERVIEW='dsp_overview'
    SA_OVERVIEW='sa_overview'
    SA_OVERVIEW_ADVERTISED_SALES='sa_overview_advertised_sales'
    SA_OVERVIEW_TOTAL_SALES='sa_overview_total_sales'


class ScopeType(str, enum.Enum):
    ADMIN='admin'
    READ='read'
    WRITE='write'


class StateType(str, enum.Enum):
    ARCHIVED='archived'
    ENABLED='enabled'
    PAUSED='paused'


# TODO(declan.ryan@getvisibly.com) Replace with coherent naming convention
class TableType(str, enum.Enum):
    ALL='ALL'
    SALES_AND_SPEND='sales_and_spend'
    SALES_AND_SPEND_BY_OBJECTIVES='sales_and_spend_by_objectives'
    TOTAL_SALES_AND_SPEND='TOTAL_SALES_AND_SPEND'


class UserStatusType(str, enum.Enum):
    ACTIVE='active'
    DISABLED='disabled'
    PENDING='pending'
