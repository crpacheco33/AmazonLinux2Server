from bson.objectid import ObjectId

from server.overrides.dict_override import Keypath
from server.resources.types.data_types import (
    AdType,
    PlatformType,
    RegionType,
)


class Daypart():
    """Model class for a dayparting schedule.

    `Daypart` represents the hours of the week during which an
    advertising campaign is enabled (delivering ads) and paused
    (not delivering ads).

    A daypart belongs to a `brand`.
    """
    
    @staticmethod
    def create(data, client):
        with client.start_session() as session:
            collection = client.visibly.dayparts
            result = collection.insert_one(
                data,
            )
            daypart = collection.find_one(
                { '_id': result.inserted_id },
            )

        if daypart:
            return Keypath(daypart)

        return None
    
    @staticmethod
    def find(ad_type: AdType, advertiser_id: str, campaign_id: str, platform: PlatformType, region: RegionType, client):
        with client.start_session() as session:
            collection = client.visibly.dayparts
            daypart = collection.find_one(
                {
                    'ad_type': ad_type,
                    'advertiser_id': advertiser_id,
                    'campaign_id': campaign_id,
                    'platform': platform,
                    'region': region,
                },
            )

            if daypart is not None:
                return Keypath(daypart)

            return None

    @staticmethod
    def find_all(ad_type: AdType, advertiser_id: str, campaign_ids: str, platform: PlatformType, region: RegionType, client):
        with client.start_session() as session:
            collection = client.visibly.dayparts
            dayparts = collection.find(
                {
                    'ad_type': ad_type,
                    'advertiser_id': advertiser_id,
                    'campaign_id': {
                        '$in': campaign_ids,
                    },
                    'platform': platform,
                    'region': region,
                },
            )

            if dayparts:
                return [Keypath(daypart) for daypart in dayparts]

            return []

    @staticmethod
    def update(daypart_id: ObjectId, data, client):
        with client.start_session() as session:
            collection = client.visibly.dayparts
            daypart = collection.find_one_and_update(
                { '_id': daypart_id },
                data,
            )

            if daypart:
                return Keypath(daypart)

            return None

    @staticmethod
    def delete(daypart_id: ObjectId, client):
        with client.start_session() as session:
            collection = client.visibly.dayparts
            collection.delete_one(
                { '_id': daypart_id },
            )