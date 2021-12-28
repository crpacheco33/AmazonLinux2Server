from bson.objectid import ObjectId

import pymongo

from server.overrides.dict_override import Keypath
from server.services.aws_service import AWSService


log = AWSService().log_service

# TODO(declan.ryan@getvisibly.com) Include company model reference where necessary.
class Brand():
    """Model class to an advertiser's brand.

    A `brand` belongs to a `company` and has many `dayparts`, `insights`,
    `reports`, `tags`, and `users`.
    """

    @staticmethod
    def add_user(brand_id: ObjectId, user_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.brands
            collection.update(
                { '_id': brand_id },
                { '$addToSet': { 'users': user_id } },
            )
            brand = collection.find_one(
                { '_id': brand_id }
            )
            
            return Keypath(brand)
    
    @staticmethod
    def create(brand, client):
        brand.update({
            'dayparts': [],
            'insights': [],
            'reports': [],
            'tags': [],
            'users': [],
        })
        with client.start_session() as session:
            collection = client.visibly.brands
            collection.insert_one(
                brand,
            )

    @staticmethod
    def create_many(brands, client):
        for brand in brands:
            brand.update({
                'dayparts': [],
                'insights': [],
                'reports': [],
                'tags': [],
                'users': [],
            })
        
        with client.start_session() as session:
            collection = client.visibly.brands
            try:
                collection.insert_many(
                    brands,
                )
            except pymongo.errors.OperationFailure as e:
                log.exception(e)
    
    @staticmethod
    def find_by_id(brand_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.brands
            brand = collection.find_one(
                { '_id': brand_id },
            )

            if brand:
                return Keypath(brand)

            return None

    @staticmethod
    def find_by_name(name, client):
        with client.start_session() as session:
            collection = client.visibly.brands
            brand = collection.find_one(
                { 'name': name },
            )

            if brand:
                return Keypath(brand)

            return None

    @staticmethod
    def find_by_id_and_user(brand_id: ObjectId, user_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.brands
            brand = collection.find_one(
                {
                    '$and': [
                        { '_id': brand_id },
                        { 'users': user_id },
                    ],
                }
            )

            if brand:
                return Keypath(brand)

            return None

    @staticmethod
    def remove_user(brand_id: ObjectId, user_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.brands
            collection.update(
                { '_id': brand_id },
                {
                    '$pull': { 'users': user_id },
                },
            )

    @staticmethod
    def with_user(user_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.brands
            brands = collection.find(
                { 'users': user_id },
            )

            if brands:
                return [Keypath(brand) for brand in list(brands)]

            return None
