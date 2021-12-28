import datetime

from fastapi import APIRouter
from fastapi.params import (
    Depends,
)

from server.core.constants import Constants
from server.dependencies import (
    advertising_data,
    brand,
    read,
)
from server.resources.models.brand import Brand
from server.resources.types.data_types import (
    ApiType,
    IntervalType,
    TableType,
)
from server.services.aws_service import AWSService
from server.services.data_service import DataService


log = AWSService().log_service
router = APIRouter()


@router.get(
    Constants.SALES_AND_SPEND_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
def sales_and_spend(
    api: ApiType,
    from_date: datetime.date,
    to_date: datetime.date,
    brand: Brand = Depends(
        brand,
    ),
    source: DataService = Depends(
        advertising_data,
    ),
):
    log.info(
        f'Obtaining advertising sales and spend...',
    )

    data = source.sales_and_spend(
        api,
        from_date,
        to_date,
    )

    log.info(
        f'Obtained advertising sales and spend',
    )

    return data


@router.get(
    Constants.SALES_AND_SPEND_BY_OBJECTIVE_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
def sales_and_spend_by_objective(
    api: ApiType,
    from_date: datetime.date,
    to_date: datetime.date,
    source: DataService = Depends(
        advertising_data,
    ),
):
    log.info(
        f'Obtaining sales and spend by objective...',
    )

    data = source.sales_and_spend_by_objective(
        api,
        from_date,
        to_date,
    )

    log.info(
        f'Obtained sales and spend by objective',
    )

    return data
    

# TODO(declan.ryan@getvisibly.com) This is no longer used, so remove.
@router.get(
    Constants.SPONSORED_ADS_AND_DSP_SALES_VS_ROAS_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
def sa_and_dsp_sales_vs_roas(
    from_date: datetime.date,
    to_date: datetime.date,
    interval: IntervalType,
    brand: Brand = Depends(
        brand,
    ),
    source: DataService = Depends(
        advertising_data,
    ),
):
    log.info(
        f'Obtaining Sponsored Ads and DSP sales versus RoAS...',
    )

    data = source.sa_and_dsp_sales_vs_roas(
        from_date,
        to_date,
        interval,
    )

    log.info(
        f'Obtained Sponsored Ads and DSP sales versus RoAS',
    )

    return data


@router.get(
    f'/aa{Constants.STATISTICS_PREFIX}',
    dependencies=[
        Depends(read),
    ],
)
def statistics(
    api: ApiType,
    from_date: str,
    to_date: str,
    table_type: TableType,
    interval: IntervalType = IntervalType.DAY,
    brand: Brand = Depends(
        brand,
    ),
    source: DataService = Depends(
        advertising_data,
    ),
):
    log.info(
        f'Obtaining Advertising statistics...',
    )

    data = source.advertising_statistics(
        api,
        from_date,
        to_date,
        interval.value,
        table_type,
    )

    log.info(
        f'Obtained Advertising statistics...',
    )

    return data
    