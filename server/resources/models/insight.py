from typing import List

import enum

from bson.objectid import ObjectId
from pymongo import ReturnDocument

import pymongo

from server.overrides.dict_override import Keypath


class Insight():
    """Model class for an insight.

    An insight is created automatically or by users and describe insights and
    actions for an advertiser to discover and respond to. `Insight` describes
    automated and manual insights, however, `server` only manages manual
    insights created by users.

    An `insight` belongs to a `brand` and a `user`, optionally to a `report`,
    and can have many `tag`s.
    """

    @staticmethod
    def add_report(insight_ids: List[ObjectId], report_id: ObjectId, brand_id: ObjectId, client):
        for insight_id in insight_ids:
            with client.start_session() as session:
                collection = client.visibly.insights
                insight = collection.find_one(
                    { '_id': insight_id, 'brand_id': brand_id }
                )

                if insight is None:
                    continue
                
                collection.update(
                    { '_id': insight.get('_id') },
                    { '$addToSet': {
                        'reports': report_id,
                    }}
                )
    
    @staticmethod
    def add_tags(insight_id: ObjectId, tags: List[str], client):
        tags = [ObjectId(tag) for tag in tags]
        
        with client.start_session() as session:
            collection = client.visibly.insights
            collection.update(
                { '_id': insight_id },
                { '$addToSet': { 'tags': { '$each': tags } } },
            )
            insight = collection.find_one(
                { '_id': insight_id }
            )
            
            return Keypath(insight)
    
    @staticmethod
    def create(data, client):
        data.update(
            {
                'reports': data.get('reports', []),
                'tags': data.get('tags', []),
            }
        )
        with client.start_session() as session:
            collection = client.visibly.insights
            result = collection.insert_one(
                data,
            )
            insight = collection.find_one(
                { '_id': result.inserted_id },
            )

        if insight:
            return Keypath(insight)

        return None
            
    @staticmethod
    def delete(insight_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.insights
            collection.remove(
                { '_id': insight_id },
            )
    
    @staticmethod
    def find(query: dict, client):
        with client.start_session() as session:
            collection = client.visibly.insights
            insight = collection.find_one(
                query,
            )
            
        if insight:
            return Keypath(insight)

        return None
    
    @staticmethod
    def find_between_dates(from_date, to_date, brand_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.insights
            insights = collection.find(
                { 
                    'date': { '$gte': from_date, '$lte': to_date },
                    'brand_id': brand_id,
                },
            )
            
        if insights:
            return [Keypath(insight) for insight in list(insights)]

        return []
    
    @staticmethod
    def find_by_id(insight_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.insights
            insight = collection.find_one(
                { '_id': insight_id },
            )

            if insight:

                insight_tags = []
                insight_tag_ids = insight.get('tags')
                for insight_tag_id in insight_tag_ids:
                    collection = client.visibly.tags
                    tag = collection.find_one(
                        { '_id': ObjectId(insight_tag_id) },
                        { '_id': 1, 'name': 1 },
                    )

                    if tag:
                        tag['id'] = str(tag.pop('_id'))
                        insight_tags.append(
                            Keypath(tag),
                        )

                insight['tags'] = insight_tags
                
                return Keypath(insight)

        return None

    @staticmethod
    def find_one(insight_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.insights
            insight = collection.find_one(
                { '_id': insight_id },
            )

        if insight:
            return Keypath(insight)

        return None

    @staticmethod
    def find_one_and_update(insight_id: ObjectId, update: dict, client):
        with client.start_session() as session:
            collection = client.visibly.insights
            insight = collection.find_one_and_update(
                { '_id': insight_id },
                update,
            )

        if insight:
            return Keypath(insight)

        return None

    @staticmethod
    def remove_report(insight_id: ObjectId, report_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.insights
            collection.update(
                { '_id': insight_id },
                {
                    '$pull': {
                        'reports': report_id,
                    }
                },
            )
    
    @staticmethod
    def update(insight_id: ObjectId, data: dict, client):
        with client.start_session() as session:
            collection = client.visibly.insights
            insight = collection.find_one_and_update(
                { '_id': insight_id },
                data,
                return_document=ReturnDocument.AFTER,
            )

        if insight:
            return Keypath(insight)

        return None
    
    @staticmethod
    def with_brand(brand_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.insights
            insights = collection.find(
                { 'brand_id': brand_id },
            ).sort(
                [('date', pymongo.DESCENDING,)],
            )

        tagged_insights = []
        for insight in insights:
            insight_tags = []
            insight_tag_ids = insight.get('tags', [])
            for insight_tag_id in insight_tag_ids:
                collection = client.visibly.tags
                tag = collection.find_one(
                    { '_id': ObjectId(insight_tag_id) },
                    { '_id': 1, 'name': 1 },
                )

                if tag:
                    tag['id'] = str(tag.pop('_id'))
                    insight_tags.append(
                        Keypath(tag),
                    )

            insight['tags'] = insight_tags
            tagged_insights.append(insight)

        if insights:
            return [Keypath(insight) for insight in tagged_insights]

        return None
    