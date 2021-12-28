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
    Constants.ADVERTISING_SALES_VERSUS_TOTAL_SALES_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
def advertising_sales_vs_total_sales(
    api: ApiType,
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
        f'Obtaining total versus advertising sales...',
    )

    data = source.advertising_sales_and_total_sales(
        api,
        from_date,
        to_date,
        interval,
    )

    log.info(
        f'Obtained total versus advertising sales',
    )
    
    return data


@router.get(
    Constants.CUMULATIVE_SALES_AND_SPEND_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
def cumulative_sales_and_spend(
    api: ApiType,
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
        f'Obtaining cumulative sales and spend...',
    )

    data = source.cumulative_sales_and_spend(
        api,
        from_date,
        to_date,
        interval,
    )

    log.info(
        f'Obtained cumulative sales and spend',
    )

    return data


@router.get(
    Constants.ENGAGEMENT_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
def engagement(
    api: ApiType,
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
        f'Obtaining engagement data...',
    )

    data = source.engagement(
        api,
        from_date,
        to_date,
        interval,
    )

    log.info(
        f'Obtained engagement data',
    )

    return data


@router.get(
    f'/my_dashboard{Constants.STATISTICS_PREFIX}',
    dependencies=[
        Depends(read),
    ],
)
def statistics(
    api: ApiType,
    from_date: datetime.date,
    to_date: datetime.date,
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
        f'Obtaining statistics...',
    )

    data = source.my_dashboard_statistics(
        api,
        from_date,
        to_date,
        interval,
        table_type,
    )

    log.info(
        f'Obtained statistics',
    )

    return data


@router.get(
    Constants.TOTAL_ATTRIBUTED_SALES_AND_SPEND_PREFIX,
    dependencies=[
        Depends(read),
    ]
)
def total_attributed_sales_and_spend(
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
        f'Obtaining total attributed sales and spend...',
    )

    data = source.total_attributed_sales_and_spend(
        api,
        from_date,
        to_date,
    )

    log.info(
        f'Obtained total attributed sales and spend',
    )

    return data
