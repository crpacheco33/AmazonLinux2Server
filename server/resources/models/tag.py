from typing import List

from bson.objectid import ObjectId
from pymongo import ReturnDocument

from server.overrides.dict_override import Keypath


class Tag():
    """Model class for tags.

    A tag is used to tag `insight`s and to create custom
    groupings of advertising and retail data, e.g., campaigns.

    A `tag` belongs to many campaigns, orders, and `insight`s.
    """

    @staticmethod
    def create(data, client):
        data.update(
            {
                'campaigns': data.get('campaigns', []),
                'insights': data.get('insights', []),
                'orders': data.get('orders', []),
            }
        )
        with client.start_session() as session:
            collection = client.visibly.tags
            result = collection.insert_one(
                data,
            )
            tag = collection.find_one(
                { '_id': result.inserted_id },
            )

        if tag:
            return Keypath(tag)

        return None
            
    @staticmethod
    def delete(tag_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.tags
            collection.remove(
                { '_id': tag_id },
            )
    
    @staticmethod
    def find_all(client):
        with client.start_session() as session:
            collection = client.visibly.tags
            tags = collection.find()

        if tags:
            return [Keypath(tag) for tag in tags]

        return None

    @staticmethod
    def find_by_id(tag_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.tags
            tag = collection.find_one(
                { '_id': ObjectId(tag_id) },
            )
            print(tag)

        if tag:
            return Keypath(tag)

        return None

    @staticmethod
    def add_insight(tag_ids: List[ObjectId], insight_id: ObjectId, brand_id: ObjectId, client):
        for tag_id in tag_ids:
            with client.start_session() as session:
                collection = client.visibly.tags
                tag = collection.find_one(
                    { '_id': tag_id, 'brand_id': brand_id }
                )

                if tag is None:
                    continue
                
                collection.update(
                    { '_id': tag.get('_id') },
                    { '$addToSet': {
                        'insights': insight_id,
                    }}
                )
    
    @staticmethod
    def update(tag: Keypath, data, client):
        with client.start_session() as session:
            collection = client.visibly.tags
            tag = collection.find_one_and_update(
                { '_id': tag._id },
                {
                    '$set': {
                        'name': data.get('name', tag.name),
                        'prefix': data.get('prefix', tag.prefix),
                        'campaigns': data.get('campaigns', tag.campaigns),
                        'insights': data.get('insights', tag.insights),
                        'orders': data.get('orders', tag.orders),
                    }
                },
                return_document=ReturnDocument.AFTER,
            )

        if tag:
            return Keypath(tag)

        return None
    
    @staticmethod
    def with_brand(brand_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.tags
            tags = collection.find(
                { 'brand_id': brand_id },
            )

        if tags:
            return [Keypath(tag) for tag in tags]

        return None
    
    @staticmethod
    def with_brand_groups(brand_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.tags
            tags = collection.find(
                {
                    'brand_id': brand_id,
                    'prefix': { '$ne': None },
                },
                { 'name': 1, 'prefix': 1 },
            )

        if tags:
            return [Keypath(tag) for tag in list(tags)]

        return None
