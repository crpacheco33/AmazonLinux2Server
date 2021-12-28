from typing import Optional

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
from server.utilities.data_utility import DataUtility


data_utility = DataUtility()
log = AWSService().log_service
router = APIRouter()


@router.get(
    Constants.DSP_AND_SA_OBJECTIVES_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
def dsp_and_sa_objectives(
    api: ApiType,
    from_date: datetime.date,
    to_date: datetime.date,
    interval: IntervalType,
    objectives: Optional[str] = None,
    brand: Brand = Depends(
        brand,
    ),
    source: DataService = Depends(
        advertising_data,
    ),
):
    log.info(
        f'Obtaining DSP and Sponsored Ads objectives...',
    )

    objectives, segments = data_utility.to_objectives_and_segments(objectives)

    data = source.dsp_and_sa_objectives(
        api,
        from_date,
        to_date,
        interval,
        objectives,
        segments,
    )

    log.info(
        f'Obtained DSP and Sponsored Ads objectives',
    )

    return data


@router.get(
    Constants.DSP_OBJECTIVES_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
def dsp(
    from_date: datetime.date,
    to_date: datetime.date,
    interval: IntervalType,
    objectives: Optional[str] = None,
    brand: Brand = Depends(
        brand,
    ),
    source: DataService = Depends(
        advertising_data,
    ),
):
    log.info(
        f'Obtaining DSP objectives...',
    )

    objectives, segments = data_utility.to_objectives_and_segments(objectives)

    data = source.dsp_objectives(
        from_date,
        to_date,
        interval,
        objectives,
        segments,
    )

    log.info(
        f'Obtained DSP objectives',
    )

    return data


@router.get(
    Constants.SPONSORED_ADS_OBJECTIVES_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
def sa(
    from_date: datetime.date,
    to_date: datetime.date,
    interval: IntervalType,
    objectives: Optional[str] = None,
    brand: Brand = Depends(
        brand,
    ),
    source: DataService = Depends(
        advertising_data,
    ),
):
    log.info(
        f'Obtaining Sponsored Ads objectives...',
    )

    objectives, segments = data_utility.to_objectives_and_segments(objectives)

    data = source.sa_objectives(
        from_date,
        to_date,
        interval,
        objectives,
    )

    log.info(
        f'Obtained Sponsored Ads objectives',
    )

    return data
