from typing import List

from bson.objectid import ObjectId
from fastapi import APIRouter
from fastapi.params import Depends
from starlette.exceptions import HTTPException
from starlette.status import (
    HTTP_404_NOT_FOUND,
)

import pymongo

from server.core.constants import Constants
from server.dependencies import (
    admin,
    brand,
    read,
    user,
    docdb,
    write,
)
from server.resources.models.brand import Brand
from server.resources.models.tag import Tag
from server.resources.schema.tag import (
    TagCreateSchema,
    TagEditSchema,
    TagIndexSchema,
    TagNewCampaignSchema,
    TagNewOrderSchema,
    TagNewSchema,
    TagUpdateSchema,
)

from server.services.aws_service import AWSService


log = AWSService().log_service
router = APIRouter(
    prefix=Constants.TAGS_PREFIX,
    tags=[Constants.TAGS],
)


@router.get(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(read)
    ],
    response_model=List[TagIndexSchema],
    response_model_by_alias=False,
)
def index(
    brand: Brand = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    
) -> List[TagIndexSchema]:
    log.info(
        'Indexing tags...',
    )

    tags = Tag.with_brand(
        brand._id,
        client,
    )

    for tag in tags:
        try:
            campaigns = tag.campaigns
        except KeyError:
            campaigns = []

        try:
            orders = tag.orders or []
        except KeyError:
            orders = []

        count = len(campaigns) + len(orders)
        tag.update({ 'count': count })

    log.info(
        'Indexed tags',
    )

    return tags


@router.get(
    Constants.GROUPS_PREFIX,
    dependencies=[
        Depends(read)
    ],
    response_model=List[TagIndexSchema],
    response_model_by_alias=False,
)
def index(
    brand: Brand = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    
) -> List[TagIndexSchema]:
    log.info(
        'Indexing tag groups...',
    )

    tags = Tag.with_brand_groups(
        brand._id,
        client,
    )

    if tags is None:
        return []

    
    log.info(
        'Indexed tag groups',
    )

    return tags


@router.get(
    Constants.NEW_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
def new(
    brand: Brand = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
):
    log.info(
        'Getting new tag...',
    )
    
    campaigns, orders = _campaigns_and_orders(
        brand,
        client,
    )

    response = TagNewSchema(
        campaigns=campaigns,
        orders=orders,
    )

    log.info(
        'Got new tag',
    )

    return response


@router.post(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(write),
    ],
    response_model=TagIndexSchema,
    response_model_by_alias=False,
)
def create(
    data: TagCreateSchema,
    brand: Brand = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
):
    log.info(
        f'Creating tag with name {data.name}...',
    )

    request_body = data.dict()
    request_body.update({
        'brand_id': brand._id,
    })

    tag = Tag.create(request_body, client)

    log.info(
        f'Created tag with name {tag.name}',
    )

    return tag


@router.get(
    '/{tag_id}',
    dependencies=[
        Depends(read),
    ],
    response_model=TagIndexSchema,
    response_model_by_alias=False,
)
def show(
    tag_id: str,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
) -> TagIndexSchema:
    log.info(
        f'Showing tag {tag_id}...',
    )

    try:
        tag = Tag.find_by_id(
            tag_id,
            client,
        )
    except Exception as e:
        log.exception(e)
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail=Constants.TAG_NOT_FOUND,
        )

    log.info(
        f'Showed tag {tag_id}',
    )

    return tag


@router.get(
    '/{tag_id}/edit',
    dependencies=[
        Depends(read),
    ],
)
def edit(
    tag_id: str,
    brand: Brand = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
):
    log.info(
        f'Editing tag {tag_id}...',
    )

    try:
        tag = Tag.find_by_id(
            ObjectId(tag_id),
            client,
        )
    except Exception as e:
        log.exception(e)

        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail=Constants.TAG_NOT_FOUND,
        )
    
    campaigns, orders = _campaigns_and_orders(
        brand,
        client,
    )

    log.info(
        f'Edited tag {tag_id}',
    )

    return TagEditSchema(
        tag=tag,
        campaigns=campaigns,
        orders=orders,
    )


@router.put(
    '/{tag_id}',
    dependencies=[
        Depends(write),
    ],
    response_model=TagIndexSchema,
    response_model_by_alias=False,
)
def update(
    tag_id: str,
    data: TagUpdateSchema,
    brand: Brand = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
):
    log.info(
        f'Updating tag with name {data.name}...',
    )

    request_body = data.dict()
    request_body.update({
        'brand_id': brand._id,
    })

    tag = Tag.find_by_id(ObjectId(tag_id), client)
    tag = Tag.update(tag, request_body, client)

    log.info(
        f'Updated tag with name {tag.name}',
    )

    return tag


@router.delete(
    '/{tag_id}',
    dependencies=[
        Depends(write),
    ],
)
def destroy(
    tag_id: str,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
):
    log.info(
        f'Destroying tag {tag_id}...',
    )

    try:
        Tag.delete(
            ObjectId(tag_id),
            client,
        )
    except Exception as e:
        log.exception(e)

    log.info(
        f'Destroyed tag {tag_id}',
    )


def _campaigns_and_orders(brand, client):
    dsp_orders, sa_campaigns = [], []

    try:
        with client.start_session() as session:
            collection = client.amazon[brand.amazon.aa.sa.advertiser_id]
            sa_campaigns = collection.find(
                {
                    '_path': {
                        '$in': [
                            '/api/v1/amazon/aa/sb/campaigns',
                            '/api/v1/amazon/aa/sd/campaigns',
                            '/api/v1/amazon/aa/sp/campaigns',
                        ],
                    }
                },
                { '_id': 0, '_path': 1, 'campaignId': 1, 'name': 1, 'startDate': 1, 'endDate': 1 },
            )
    except KeyError:
        pass

    try:
        with client.start_session() as session:
            collection = client.amazon[brand.amazon.aa.dsp.advertiser_id]
            dsp_orders = collection.find(
                {
                    '_path': {
                        '$in': [
                            '/api/v1/amazon/aa/dsp/orders',
                        ],
                    },
                },
                { '_id': 0, 'orderId': 1, 'name': 1, 'startDateTime': 1, 'endDateTime': 1 },
            )
    except KeyError:
        pass

    campaigns = [TagNewCampaignSchema(**campaign) for campaign in list(sa_campaigns)]
    orders = [TagNewOrderSchema(**order) for order in list(dsp_orders)]

    return campaigns, orders
