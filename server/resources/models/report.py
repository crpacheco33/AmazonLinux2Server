from typing import List

import enum

from bson.objectid import ObjectId
from pymongo import ReturnDocument

import pymongo

from server.overrides.dict_override import Keypath
from server.resources.types.data_types import ReportStateType


class Report():
    """Model class for reports.

    A report is a summary of a brand's advertising and retail position. A
    `report` belongs to a `brand` and to a `user` and can have one `insight`
    and can have many `tags`.
    """

    @staticmethod
    def add_insights(report_id: ObjectId, insights: List[str], client):
        insights = [ObjectId(insight) for insight in insights]
        
        with client.start_session() as session:
            collection = client.visibly.reports
            report = collection.find_one_and_update(
                { '_id': report_id },
                { '$addToSet': { 'insights': { '$each': insights } } },
                return_document=ReturnDocument.AFTER,
            )
            
        return Keypath(report)
    
    @staticmethod
    def create(data, client):
        data.update(
            {
                'insights': data.get('insights', []),
                'tags': data.get('tags', []),
            }
        )
        
        reports = client.visibly.reports
        with client.start_session() as session:
            with session.start_transaction():
                result = reports.insert_one(
                    data,
                    session=session,
                )
                report = reports.find_one(
                    { '_id': result.inserted_id },
                    session=session,
                )

        if report:
            return Keypath(report)

        return None
            
    @staticmethod
    def delete(report_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.reports
            collection.remove(
                { '_id': report_id },
            )
    
    @staticmethod
    def find_by_id(report_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.reports
            report = collection.find_one(
                { '_id': report_id },
            )
            
        if report:
            return Keypath(report)

        return None
    
    @staticmethod
    def with_brand(brand_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.reports
            reports = collection.find(
                { 'brand_id': brand_id },
            ).sort(
                [('start_date', pymongo.DESCENDING,)],
            )

        if reports:
            return [Keypath(report) for report in reports]

        return None
    