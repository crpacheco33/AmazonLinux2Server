from typing import List

from fastapi import (
    APIRouter,
    Request,
)
from fastapi.params import (
    Depends,
)

import pymongo

from server.core.constants import Constants
from server.dependencies import (
    brand,
    docdb,
    Interface,
    read,
    profiles_client,
    write,
)
from server.resources.models.brand import Brand
from server.resources.models.daypart import Daypart
from server.resources.schema.daypart import (
    CreateUpdateDaypartSchema,
    DaypartSchema,
)
from server.services.aws_service import AWSService
from server.utilities.aa_utility import AAUtility


klass = AAUtility().profile_interface_klass()
interface = Interface(profiles_client, klass)
log = AWSService().log_service
router = APIRouter(
    prefix=Constants.DAYPART_PREFIX,
    tags=[Constants.DAYPARTS],
)


@router.get(
    Constants.NEW_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def new(
    brand: Brand = Depends(
        brand,
    ),
    interface: Interface = Depends(
        interface,
    ),
):
    log.info(
        'Getting customer timezone...',
    )

    try:
        timezone = brand.timezone
    except Exception as e:
        log.exception(e)
        timezone = Constants.AMERICA_LOS_ANGELES_TIMEZONE
    
    log.info(
        'Got customer timezone',
    )

    return timezone


@router.put(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(write),
    ],
)
def create_or_update(
    request: Request,
    data: List[CreateUpdateDaypartSchema],
    brand: Brand = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
):
    log.info(
        'Creating or updating dayparts...',
    )

    advertiser_id = brand.amazon.aa.sa.advertiser_id
    region = brand.amazon.aa.region

    for datum in data:
        daypart = Daypart.find(
            ad_type=datum.ad_type,
            advertiser_id=advertiser_id,
            campaign_id=datum.campaign_id,
            platform=datum.platform,
            region=region,
            client=client,
        )

        if daypart:
            if datum.schedule:
                log.info(
                    f'Updating daypart {daypart._id}...'
                )

                updated_daypart = Daypart.update(
                    daypart._id,
                    {
                        '$set': {
                            'bids': datum.bids,
                            'schedule': datum.schedule,
                        },
                    },
                    client,
                )
            else:
                log.info(
                    f'Deleting daypart {daypart._id}...'
                )
                
                Daypart.delete(daypart._id, client)
        else:
            log.info(
                f'Creating daypart...'
            )
            if datum.schedule is None:
                log.warning(
                    f'Unable to create daypart without schedule',
                )
                continue

            Daypart.create(
                {
                    'ad_type': datum.ad_type,
                    'advertiser_id': advertiser_id,
                    'brand_id': brand._id,
                    'bids': datum.bids,
                    'campaign_id': datum.campaign_id,
                    'platform': datum.platform,
                    'region': region,
                    'schedule': datum.schedule,
                },
                client,
            )

    log.info(
        'Created or updated dayparts',
    )