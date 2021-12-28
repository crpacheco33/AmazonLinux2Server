from typing import Any

import re

from fastapi import APIRouter
from fastapi.params import Depends

import pymongo

from server.core.constants import Constants
from server.dependencies import (
    brand,
    docdb,
    read,
    retail_data,
)
from server.resources.types.data_types import (
    BrandAnalyticsDistributorType,
    BrandAnalyticsReportType,
    BrandAnalyticsSalesType,
    BrandAnalyticsSellingProgramType,
    IntervalType,
)
from server.services.aws_service import AWSService
from server.services.data_service import DataService
from server.utilities.brand_utility import (
    to_vendor_id,
)


log = AWSService().log_service


router = APIRouter(
    prefix=Constants.REPORTS_PREFIX,
)


@router.get(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index(
    start_date: str,
    end_date: str,
    distributor_view: BrandAnalyticsDistributorType,
    report_type: BrandAnalyticsReportType,
    selling_program: BrandAnalyticsSellingProgramType,
    interval: IntervalType,
    brand: Any = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    source: DataService = Depends(
        retail_data,
    ),
):
    log.info(
        f'Indexing brand analytics {report_type}...',
    )

    vendor_id = to_vendor_id(
        brand.amazon.sp.ba.seller_partner_id,
    )

    ba_path_pattern = re.compile('ba')

    with client.start_session() as session:
        collection = client.amazon[vendor_id]
        asin_metadata = collection.find(
            { '_path': ba_path_pattern },
            {
                '_id': 0,
                'asin': 1,
                'attributes.item_name.value' : 1,
                'attributes.product_category.value': 1,
                'attributes.brand.value': 1,
            },
        )

    asins = {}
    for asin_metadatum in asin_metadata:
        asin = asin_metadatum.get('asin')
        attributes = asin_metadatum.get('attributes', {})
        
        try:
            brand = attributes.get('brand')[0].get(
                'value',
                Constants.NO_BRAND,
            )
            category = attributes.get('product_category')[0].get(
                'value',
                Constants.NO_CATEGORY,
            )
            name = attributes.get('item_name')[0].get(
                'value',
                Constants.NO_NAME,
            )

            asins[asin] = {
                'brand': brand,
                'category': category,
                'name': name,
            }
        except Exception as e:
            log.exception(e)
            asins[asin] = {}

    data = source.brand_analytics(
        asins,
        distributor_view,
        report_type,
        selling_program,
        start_date,
        end_date,
        interval,
    )



    log.info(
        f'Indexed brand analytics {report_type}',
    )

    return data


@router.get(
    Constants.TABLE_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index_table(
    start_date: str,
    end_date: str,
    distributor_view: BrandAnalyticsDistributorType,
    report_type: BrandAnalyticsReportType,
    selling_program: BrandAnalyticsSellingProgramType,
    brand: Any = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    source: DataService = Depends(
        retail_data,
    ),
):
    log.info(
        f'Indexing brand analytics {report_type}...',
    )

    vendor_id = to_vendor_id(
        brand.amazon.sp.ba.seller_partner_id,
    )

    ba_path_pattern = re.compile('ba')

    with client.start_session() as session:
        collection = client.amazon[vendor_id]
        asin_metadata = collection.find(
            { '_path': ba_path_pattern },
            {
                '_id': 0,
                'asin': 1,
                'attributes.item_name.value' : 1,
                'attributes.product_category.value': 1,
                'attributes.brand.value': 1,
            },
        )

    asins = {}
    for asin_metadatum in asin_metadata:
        asin = asin_metadatum.get('asin')
        attributes = asin_metadatum.get('attributes', {})
        
        try:
            brand = attributes.get('brand')[0].get(
                'value',
                Constants.NO_BRAND,
            )
            category = attributes.get('product_category')[0].get(
                'value',
                Constants.NO_CATEGORY,
            )
            name = attributes.get('item_name')[0].get(
                'value',
                Constants.NO_NAME,
            )

            asins[asin] = {
                'brand': brand,
                'category': category,
                'name': name,
            }
        except Exception as e:
            log.exception(e)
            asins[asin] = {}

    data = source.brand_analytics_statistics(
        asins,
        distributor_view,
        report_type,
        selling_program,
        start_date,
        end_date,
    )

    log.info(
        f'Indexed brand analytics {report_type}',
    )

    return data
