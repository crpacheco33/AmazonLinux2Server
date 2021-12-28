from typing import List

from bson.objectid import ObjectId
from pymongo import ReturnDocument

from server.overrides.dict_override import Keypath
from server.resources.types.data_types import UserStatusType


class User:
    """Model class for a user.

    A user belongs to many `brand`s and has many `insight`s and `report`s.
    """

    @staticmethod
    def add_brand(user_id, brand_id, client):
        with client.start_session() as session:
            collection = client.visibly.users
            
            collection.update(
                { '_id': user_id },
                { '$addToSet': { 'brands': brand_id } },
            )
            user = collection.find_one(
                { '_id': user_id },
            )
            
            return Keypath(user)
    
    @staticmethod
    def create(data, client):
        data.update(
            {
                'refresh_token': None,
                'status': UserStatusType.PENDING,
                'brands': [],
                'insights': [],
                'reports': [],
            }
        )
        with client.start_session() as session:
            collection = client.visibly.users
            collection.insert_one(
                data,
            )
            
    @staticmethod
    def find(client):
        with client.start_session() as session:
            collection = client.visibly.users
            users = collection.find()
            
            if users:
                return [Keypath(user) for user in users]

            return []
    
    @staticmethod
    def find_by_email(email, client):
        with client.start_session() as session:
            collection = client.visibly.users
            user = collection.find_one(
                { 'email': email, 'status': { '$ne': UserStatusType.DISABLED } },
            )
            
            if user:
                return Keypath(user)

            return None
    
    @staticmethod
    def find_by_id(id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.users
            user = collection.find_one(
                { '_id': id }
            )

            if user:
                return Keypath(user)

            return None

    @staticmethod
    def find_by_password(password, client):
        with client.start_session() as session:
            collection = client.visibly.users
            user = collection.find_one(
                { 'password': password }
            )

            if user:
                return Keypath(user)

            return None

    @staticmethod
    def remove_brands(user_id: ObjectId, brand_ids: List[ObjectId], client):
        with client.start_session() as session:
            collection = client.visibly.users
            
            collection.update(
                { '_id': user_id },
                { '$pull': { 'brands': { '$in': brand_ids } } },
            )
    
    @staticmethod
    def update(user_id: ObjectId, data, client):
        with client.start_session() as session:
            collection = client.visibly.users
            user = collection.find_one_and_update(
                { '_id': user_id },
                data,
                return_document=ReturnDocument.AFTER,
            )

            if user:
                return Keypath(user)

            return None
