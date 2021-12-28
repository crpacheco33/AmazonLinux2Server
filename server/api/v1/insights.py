from typing import (
    Any,
    List,
)

from fastapi import APIRouter
from fastapi.params import (
    Depends,
)

from bson.objectid import ObjectId
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_403_FORBIDDEN,
    HTTP_404_NOT_FOUND,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

import pymongo

from server.core.constants import Constants
from server.dependencies import (
    admin,
    brand,
    insight_service,
    read,
    user,
    docdb,
    write,
)
from server.resources.models.brand import Brand
from server.resources.models.insight import Insight
from server.resources.models.tag import Tag
from server.resources.schema.insight import (
    InsightCreateSchema,
    InsightIndexSchema,
    InsightIndexTagNameSchema,
    InsightShowSchema,
    InsightUpdateSchema,
)
from server.resources.schema.tag import TagIndexSchema
from server.services.insight_service import InsightService
from server.resources.types.data_types import (
    InsightMetaStateType,
    InsightStateType,
    InsightType,
    ScopeType,
)
from server.services.aws_service import AWSService


log = AWSService().log_service
router = APIRouter(
    prefix=Constants.INSIGHTS_PREFIX,
    tags=[Constants.INSIGHTS],
)


@router.get(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(read),
    ],
    response_model=List[InsightIndexTagNameSchema],
    response_model_by_alias=False,
)
def index(
    brand: Brand = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
):
    log.info(
        'Indexing insights...'
    )
    
    insights = Insight.with_brand(
        brand._id,
        client,
    )
    
    log.info('Indexed insights...')

    return insights


@router.get(
    Constants.NEW_PREFIX,
    dependencies=[
        Depends(read)
    ],
    response_model=List[TagIndexSchema],
    response_model_by_alias=False,
)
def new(
    brand: Brand = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    
) -> List[TagIndexSchema]:
    log.info(
        'Indexing insight tags...',
    )

    tags = Tag.with_brand(
        brand._id,
        client,
    )

    log.info(
        'Indexed insight tags',
    )

    return tags
    

@router.post(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(write),
    ],
)
def create(
    data: InsightCreateSchema,
    brand: Brand = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    user: Any = Depends(
        user,
    ),
):
    log.info(
        f'Creating insight with description {data.description}',
    )
    
    request_body = data.dict()
    request_body.update({
        'brand_id': brand._id,
        'type': InsightType.MANUAL,
        'user_id': user._id,
    })

    tag_ids = request_body.pop(Constants.TAGS)

    tag_object_ids = [ObjectId(tag_id) for tag_id in tag_ids]

    request_body.update({ Constants.TAGS: tag_object_ids })
    
    result = None
    try:
        with client.start_session() as session:
            collection = client.visibly.insights
            result = collection.insert_one(request_body)
        
    except Exception as e:
        log.exception(e)
        
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=Constants.INSIGHT_CREATE_FAILURE,
        )

    try:
        Tag.add_insight(
            tag_object_ids,
            result.inserted_id,
            brand._id,
            client,
        )
        
    except Exception as e:
        log.exception(e)
        
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=TAG_IDS_INVALID,
        )

    log.info(
        f'Created insight with description {data.description}',
    )

    return { 'id': str(result.inserted_id) }


@router.get(
    '/{insight_id}',
    dependencies=[
        Depends(read),
    ],
    response_model=InsightIndexTagNameSchema,
    response_model_by_alias=False,
)
def show(
    insight_id: str,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
):
    log.info(
        f'Showing insight {insight_id}...',
    )

    try:
        insight = Insight.find_by_id(
            ObjectId(insight_id),
            client,
        )

    except Exception as e:
        log.exception(e)
        
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail=Constants.INSIGHT_MISSING,
        )
    
    log.info(
        f'Showed insight {insight_id}',
    )

    return insight


@router.put(
    '/{insight_id}',
    dependencies=[
        Depends(write),
    ],
    response_model=InsightIndexSchema,
    response_model_by_alias=False,
)
def update(
    insight_id: str,
    data: InsightUpdateSchema,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    user: Any = Depends(
        user,
    ),
):
    log.info(
        f'Updating insight {insight_id}...',
    )
    request_body = data.dict()

    tag_ids = request_body.pop(Constants.TAGS)

    try:
        insight = Insight.find_by_id(
            ObjectId(insight_id),
            client,
        )

        if insight is None:
            raise HTTPException(
                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                detail=Constants.INSIGHT_MISSING,
            )

        if insight.type == InsightType.MANUAL:
            if insight.user_id == user._id:
                log.info(
                    f'User {user._id} updating insight {insight_id}...',
                )
            elif Scope.ADMIN in [scope for scope in user.scopes]:
                log.info(
                    f'Admin user {user._id} updating insight {insight_id}...',
                )
            else:
                raise HTTPException(
                    HTTP_403_FORBIDDEN,
                    detail=Constants.INSUFFICIENT_PERMISSIONS,
                )

        insight = Insight.find_one_and_update(
            insight._id,
            { '$set': request_body },
            client,
        )

        # Add new tags to insight...
        insight = Insight.add_tags(
            insight._id,
            tag_ids,
            client,
        )
        
        # ...identify the insight's tags that only come from tag_ids...
        insight_tags = insight.tags
        tags = {
            insight_tag for insight_tag in insight_tags if str(insight_tag) in tag_ids
        }

        # ...update the insight with those tags only...
        insight = Insight.find_one_and_update(
            insight._id,
            { '$set' : { 'tags': list(tags) } },
            client,
        )
        
    except Exception as e:
        log.exception(e)

        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=Constants.INSIGHT_UPDATE_FAILURE,
        )

    log.info(
        f'Updated insight {insight_id}',
    )

    insight = Insight.find_by_id(insight._id, client)

    return insight


@router.put(
    '/{insight_id}/accept',
    dependencies=[
        Depends(write),
    ],
    response_model=InsightIndexSchema,
    response_model_by_alias=False,
)
def accept(
    insight_id: str,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    insight_service: InsightService = Depends(
        insight_service,
    )
):
    log.info(
        f'Accepting insight {insight_id}...',
    )

    insight = Insight.find(
        {
            '_id': ObjectId(insight_id),
            'type': InsightType.AUTOMATED,
        },
        client,
    )

    if insight is None:
        raise HTTPException(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            detail=Constants.INSIGHT_MISSING,
        )

    meta_insight = dict(insight.meta)
    automated_insight_state = meta_insight.get(
        Constants.STATE,
    )

    if automated_insight_state is not None:
        
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=Constants.INSIGHT_FAILED_TO_ACCEPT,
        )

    try:
        insight_service.accept(meta_insight)
    except Exception as e:
        log.exception(e)
        
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    try:
        insight = Insight.update(
            ObjectId(insight_id),
            {
                '$set': {
                    'meta.state': InsightMetaStateType.ACCEPTED,
                }
            },
            client,
        )
        
    except Exception as e:
        log.exception(e)
        
        raise HTTPException(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            detail=Constants.INSIGHT_AUTOMATED_UPDATE_FAILURE,
        )

    log.info(
        f'Accepted insight {insight_id}',
    )
    return insight


@router.put(
    '/{insight_id}/dismiss',
    dependencies=[
        Depends(write),
    ],
    response_model=InsightIndexSchema,
    response_model_by_alias=False,
)
def dismiss(
    insight_id: str,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    insight_service: InsightService = Depends(
        insight_service,
    ),
):
    log.info(
        f'Dismissing insight {insight_id}...',
    )

    insight = Insight.find(
        {
            '_id': ObjectId(insight_id),
            'type': InsightType.AUTOMATED,
        },
        client,
    )

    if insight is None:
        raise HTTPException(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            detail=Constants.INSIGHT_MISSING,
        )

    meta_insight = dict(insight.meta)
    automated_insight_state = meta_insight.get(
        Constants.STATE,
    )

    if automated_insight_state is not None:
        
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=Constants.INSIGHT_FAILED_TO_ACCEPT,
        )
    elif automated_insight_state == InsightMetaStateType.DISMISSED:
        log.info(
            f'Already dismissed {insight_id}',
        )
        return InsightIndexSchema.parse_obj(insight)

    try:
        insight_service.dismiss(meta_insight)
    except Exception as e:
        log.exception(e)
        
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    try:
        insight = Insight.update(
            ObjectId(insight_id),
            {
                '$set': {
                    'meta.state': InsightMetaStateType.DISMISSED,
                }
            },
            client,
        )
        
    except Exception as e:
        log.exception(e)
        
        raise HTTPException(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            detail=Constants.INSIGHT_AUTOMATED_UPDATE_FAILURE,
        )

    log.info(
        f'Dismissed insight {insight_id}',
    )
    return insight


@router.put(
    '/{insight_id}/like',
    dependencies=[
        Depends(write),
    ],
    response_model=InsightIndexSchema,
    response_model_by_alias=False,
)
def like(
    insight_id: str,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
):
    log.info(
        f'Liking insight {insight_id}...',
    )

    insight = Insight.update(
        ObjectId(insight_id),
        {
            '$set': {
                'state': InsightStateType.FAVORITE,
            }
        },
        client,
    )
    
    if insight is None:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail=Constants.INSIGHT_MISSING,
        )

    log.info(
        f'Liked insight {insight_id}',
    )
    return InsightIndexSchema.parse_obj(insight)


@router.put(
    '/{insight_id}/unlike',
    dependencies=[
        Depends(write),
    ],
    response_model=InsightIndexSchema,
    response_model_by_alias=False,
)
def unlike(
    insight_id: str,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
):
    log.info(
        f'Unliking insight {insight_id}...',
    )

    insight = Insight.update(
        ObjectId(insight_id),
        {
            '$set': {
                'state': None,
            }
        },
        client,
    )
    
    if insight is None:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail=Constants.INSIGHT_MISSING,
        )

    log.info(
        f'Unliked insight {insight_id}',
    )
    return insight


@router.delete(
    '/{insight_id}',
    dependencies=[
        Depends(write),
    ],
)
def destroy(
    insight_id: str,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    user: Any = Depends(
        user,
    ),
):
    log.info(
        f'Deleting insight {insight_id}...',
    )
    insight = Insight.find_by_id(
        ObjectId(insight_id),
        client,
    )

    if insight is None:
        raise HTTPException(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            detail=Constants.INSIGHT_MISSING,
        )

    if insight.type == InsightType.AUTOMATED:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail=Constants.INSIGHT_CANNOT_DELETE_AUTOMATED,
        )

    if insight.user_id == user._id:
        log.info(
            f'User {user._id} deleting insight {insight._id}...',
        )
    elif ScopeType.ADMIN in [scope for scope in user.scopes]:
        log.info(
            f'Admin user {user._id} deleting insight {insight._id}...',
        )
    else:
        log.info(
            f'User {user._id} does not have correct permissions',
        )
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail=Constants.INSUFFICIENT_PERMISSIONS,
        )

    Insight.delete(insight._id, client)

    log.info(
        f'Deleted insight {insight_id}',
    )
