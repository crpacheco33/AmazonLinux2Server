from fastapi import APIRouter
from fastapi.params import Depends

from server.core.constants import Constants
from server.dependencies import (
    read,
    retail_data,
)
from server.resources.schema.amazon_api import (
    IndexSearchTermsRankingSchema,
    IndexSearchTermsSchema,
)
from server.resources.types.data_types import (
    BrandAnalyticsDistributorType,
    BrandAnalyticsIntervalType,
    IntervalType,
)
from server.services.aws_service import AWSService
from server.services.data_service import DataService


log = AWSService().log_service


router = APIRouter(
    prefix=Constants.SEARCH_TERMS_PREFIX,
)


@router.get(
    Constants.SEARCH_TERMS_PERIODS_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index_periods(
    source: DataService = Depends(
        retail_data,
    ),
):
    log.info(
        f'Indexing search terms periods...',
    )

    data = source.search_term_periods()

    log.info(
        f'Indexed search terms periods',
    )

    return data


@router.get(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index_search_terms_filter(
    q: str,
    limit: int = 20,
    source: DataService = Depends(
        retail_data,
    ),
):
    log.info(
        f'Indexing search terms filter...',
    )

    data = source.search_terms_filter(
        q,
        limit,
    )

    log.info(
        f'Indexed search terms filter',
    )

    return data


@router.post(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index_search_terms(
    data: IndexSearchTermsSchema,
    source: DataService = Depends(
        retail_data,
    ),
):
    log.info(
        f'Indexing search terms...',
    )
    
    data = source.search_terms(data)

    log.info(
        f'Indexed search terms',
    )

    return data


@router.post(
    Constants.SEARCH_TERMS_RANK_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index_search_terms_rank(
    data: IndexSearchTermsRankingSchema,
    source: DataService = Depends(
        retail_data,
    ),
):
    log.info(
        f'Indexing search terms rank...',
    )

    data = source.search_terms_rank(data)

    log.info(
        f'Indexed search terms rank',
    )

    return data
