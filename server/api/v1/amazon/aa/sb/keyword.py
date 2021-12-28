from typing import (
    Any,
    List,
)

import json

from fastapi import (
    APIRouter,
    HTTPException,
    Request,
    Response,
)
from fastapi.params import (
    Depends,
)
from starlette.status import (
    HTTP_422_UNPROCESSABLE_ENTITY,
)

from server.core.constants import Constants
from server.dependencies import (
    advertising_data,
    brand,
    Interface,
    read,
    sb_client,
    write,
)
from server.middleware.aa_middleware import AAMiddleware
from server.resources.schema.amazon_api import (
    APIIndexSchema,
    UpdateSBKeywordSchema,
)
from server.resources.types.data_types import (
    CreativeType,
    KeywordMatchType,
)
from server.services.aws_service import AWSService
from server.services.data_service import DataService
from server.utilities.aa_utility import AAUtility
from server.utilities.list_utility import partition_list


klass = AAUtility().keyword_interface_klass(
    Constants.SPONSORED_BRANDS,
)
interface = Interface(sb_client, klass)
log = AWSService().log_service
router = APIRouter(
    prefix=Constants.KEYWORDS_PREFIX,
    route_class=AAMiddleware,
)


@router.get(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index(
    request: Request,
    response: Response,
    from_date: str = None,
    to_date: str = None,
    keywordText: str = None,
    creativeType: CreativeType = None,
    matchTypeFilter: KeywordMatchType = None,
    stateFilter: str = None,
    adGroupIdFilter: str = None,
    campaignIdFilter: str = None,
    keywordIdFilter: str = None,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
    source: DataService = Depends(
        advertising_data,
    ),
):
    log.info(
        f'Indexing SB keywords...',
    )

    keywords = await interface.index(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=Response,
    )
    total=len(keywords)
    
    keyword_ids = [str(keyword.get('keywordId')) for keyword in keywords]

    data = {}
    partitions = partition_list(keyword_ids, Constants.ES_FILTER_ARRAY_LIMIT)

    for partition in partitions:
        es_data = source.sa_model(
            Constants.SPONSORED_BRANDS,
            Constants.KEYWORD,
            partition,
            from_date,
            to_date,
        )
        data.update(es_data)

    response = []

    for keyword in keywords:
        keyword_id = str(keyword.get('keywordId'))
        if keyword_id in data:
            keyword.update(
                data.get(keyword_id),
            )
        
        response.append(keyword)
    

    log.info(
        f'Indexed SB keywords',
    )

    return APIIndexSchema(
        data=response,
        total_count=total,
    )


@router.post(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(write),
    ],
)
async def create(
    request: Request,
    data: List[Any],
):
    pass


@router.get(
    '/{keyword_id}',
    dependencies=[
        Depends(read),
    ],
)
async def show(
    request: Request,
    response: Response,
    keyword_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Showing SB keyword {keyword_id}...',
    )

    response = await interface.show(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=keyword_id,
        request=request,
        response=response,
    )
    
    log.info(
        f'Showed SB keyword {keyword_id}',
    )

    return response


@router.put(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(write),
    ],
)
async def update(
    request: Request,
    data: List[UpdateSBKeywordSchema],
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Updating SB keywords...',
    )

    data = [
        json.loads(
            datum.json(
                exclude_none=True,
                exclude_defaults=True,
            ),
        )
        for datum in data
    ]

    response = await interface.update(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        data=data,
        request=request,
    )

    keywords, keyword_ids = [], []
    try:
        keyword_ids = [keyword.get('keywordId') for keyword in response]
    except AttributeError as e:
        log.exception(e)
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(response),
        )

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sb/keywords',
            'query_string': b'',
            'type': 'http',
        }
    )

    for keyword_id in keyword_ids:
        # GETting the keyword without a Response object will update
        # the object's cache
        keyword = await interface.show(
            advertiser_id=brand.amazon.aa.sa.advertiser_id,
            key=keyword_id,
            request=request,
        )
        keywords.append(keyword)

    log.info(
        f'Updated SB keywords',
    )

    return keywords


@router.delete(
    '/{keyword_id}',
    dependencies=[
        Depends(write),
    ],
)
async def destroy(
    request: Request,
    keyword_id: str,
    brand: Any = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        f'Archiving SB keyword {keyword_id}...',
    )

    response = await interface.destroy(
        advertiser_id=brand.amazon.advertiser_id,
        key=keyword_id,
        request=request,
    )

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sb/keywords',
            'query_string': b'',
            'type': 'http',
        }
    )

    keyword = await interface.show(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=keyword_id,
        request=request,
    )
        
    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': '/api/v1/amazon/aa/sb/keywords',
            'query_string': b'',
            'type': 'http',
        }
    )

    # GETting the keyword without a Response object will update
    # the object's cache
    keyword = await interface.show(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        key=keyword_id,
        request=request,
    )
    
    log.info(
        f'Archived SB keyword {keyword_id}',
    )

    return keyword
