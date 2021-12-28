"""Decorates API requests to third-party APIs.

The decorator returns data from DocumentDB for all GET requests and returns
data from the third-party API in all other instances. Although Redis is also
supported, it is likely to be deprecated and removed in favor of DocumentDB.
"""


from collections import defaultdict
from datetime import datetime
from functools import wraps
from typing import (
    Callable,
)

import re

from starlette.status import HTTP_304_NOT_MODIFIED

import requests

from server.core.constants import Constants
from server.services.aws_service import AWSService
from server.services.redis_service import RedisService
from server.utilities.cache_utility import CacheUtility
from server.utilities.data_utility import DataUtility


cache_utility = CacheUtility()
data_utility = DataUtility()
log = AWSService().log_service
redis_service = RedisService().cache


def docdb_cache(is_many: bool = True):
    """Gets data from a DocumentDB cache or requests an external API.

    Args:
        is_many: Whether the response is a list (True) or a single dictionary
        (False)

    Returns:
        Cached response from DocumentDB, or the non-cached response from the API
    """
    def wrapper(func):
        @wraps(func)
        async def inner(self, *args, **kwargs):
            aws_service = AWSService()
            docdb_service = aws_service.docdb_service

            # Remove non-API keyword arguments before making a request
            wrapped_kwargs = kwargs.copy()
            # `advertiser_id` is the name of the collection (Amazon's Sponsored Ads API)
            advertiser_id = wrapped_kwargs.pop(Constants.ADVERTISER_ID, None)
            # `dsp_advertiser_id` is the name of the collection (Amazon's DSP API)
            dsp_advertiser_id = kwargs.pop(Constants.DSP_ADVERTISER_ID, None)
            data = wrapped_kwargs.pop(Constants.DATA, None)
            key = wrapped_kwargs.pop(Constants.KEY, None)
            request = wrapped_kwargs.pop(Constants.REQUEST, None)
            response = wrapped_kwargs.pop(Constants.RESPONSE, None)
            
            if request.method != Constants.GET:
                return await func(
                    self,
                    advertiser_id=advertiser_id,
                    data=data,
                    request=request,
                    *args,
                    **wrapped_kwargs,
                )

            model = None
            path = request.url.path

            # `key` is an object identifier, e.g., a campaign ID
            if 'dsp' not in path and key:
                key = key

            database = docdb_service.client.amazon
            
            if key:
                # Cache uses a `_path` field without any `key` as a cache key
                path = path.replace(f'/{key}', Constants.EMPTY_STRING)
                model = path.split(
                    Constants.FORWARD_SLASH,
                )[-1]
                # Model is extracted to create the identifier's field name
                model = data_utility.to_camel_case(
                    data_utility.to_singular(
                        model,
                    )
                )
                query = { '_path': path, f'{model}Id': key, }
                value = database[advertiser_id].count_documents(
                    query,
                )
                if dsp_advertiser_id:
                    value = database[dsp_advertiser_id].count_documents(
                        query,
                    )
            else:
                query = { '_path': path, }
            
            value = database[advertiser_id].find(query)
            if dsp_advertiser_id:
                log.info(
                    f'DSP advertiser_id {dsp_advertiser_id} included in kwargs',
                )
                value = database[dsp_advertiser_id].find(query)
            
            if value and response:
                query_parameters = {**request.query_params}
                ad_group_ids = query_parameters.get('adGroupIdFilter')
                ad_ids = query_parameters.get('adIdFilter')
                asin = query_parameters.get('asin')
                campaign_ids = query_parameters.get('campaignIdFilter')
                creative_line_item_ids = query_parameters.get('creativeLineItemIdFilter')
                device_types = query_parameters.get('deviceTypes')
                expression_type = query_parameters.get('expressionType')
                from_date = query_parameters.get('from_date')
                geo_locations = query_parameters.get('geoLocationIdFilter')
                keyword_text = query_parameters.get('keywordText')
                line_item_ids = query_parameters.get('lineItemIdFilter')
                line_item_type = query_parameters.get('lineItemType')
                name = query_parameters.get('name')
                order_ids = query_parameters.get('orderIdFilter')
                portfolio_ids = query_parameters.get('portfolioIdFilter')
                q = query_parameters.get('query')
                target_ids = query_parameters.get('targetIdFilter')
                to_date = query_parameters.get('to_date')
                state = query_parameters.get('stateFilter')
                supply_source_type = query_parameters.get('supplySourceType')

                query = defaultdict(list)
                query['_path'] = path

                if ad_group_ids:
                    ad_group_ids = ad_group_ids.split(Constants.COMMA)
                    ad_group_ids = [ad_group_id for ad_group_id in ad_group_ids]
                    query['adGroupId'] = {
                        '$in': ad_group_ids,
                    }

                if ad_ids:
                    ad_ids = ad_ids.split(Constants.COMMA)
                    ad_ids = [ad_id for ad_id in ad_ids]
                    query['adId'] = {
                        '$in': ad_ids,
                    }

                if asin:
                    query['asin'] = {
                        '$regex': asin,
                    }
                
                if campaign_ids:
                    campaign_ids = campaign_ids.split(Constants.COMMA)
                    campaign_ids = [campaign_id for campaign_id in campaign_ids]
                    query['campaignId'] = {
                        '$in': campaign_ids,
                    }

                if creative_line_item_ids:
                    creative_line_item_ids = creative_line_item_ids.split(Constants.COMMA)
                    
                    items = database[advertiser_id].find(
                        {
                            '_path': '/api/v1/amazon/aa/dsp/line_item_creative_associations',
                            'lineItemId': {
                                '$in': creative_line_item_ids,
                            },
                        },
                        { 'creativeId': 1, '_id': 0 },
                    )
                    creative_ids = [item.get('creativeId') for item in items]

                    query['creativeId'] = {
                        '$in': creative_ids,
                    }
                
                if target_ids:
                    target_ids = target_ids.split(Constants.COMMA)
                    target_ids = [target_id for target_id in target_ids]
                    query['targetId'] = {
                        '$in': target_ids,
                    }
                
                if not any([ad_group_ids, campaign_ids, creative_line_item_ids, line_item_ids, order_ids, target_ids]):
                    if from_date and to_date:
                        if 'dsp' in path:
                            start_date = datetime.strptime(
                                from_date,
                                Constants.DATE_FORMAT_YYYYMMDD,
                            )
                            start_date_string = datetime.strftime(
                                start_date,
                                Constants.DATE_FORMAT_DSP,
                            )
                            end_date = datetime.strptime(
                                to_date,
                                Constants.DATE_FORMAT_YYYYMMDD,
                            )
                            end_date_string = datetime.strftime(
                                end_date,
                                Constants.DATE_FORMAT_DSP,
                            )

                            # Find objects between from_date (start_date_string) and end_date(end_date_string)
                            query['$and'] = [
                                {
                                    'startDateTime': { '$lte': end_date_string },
                                },
                                { 
                                    '$or': [
                                        {
                                            'endDateTime': { '$exists': False },
                                        },
                                        {
                                            '$and': [
                                                {
                                                    'endDateTime': { '$gte': start_date_string },
                                                },
                                            ],
                                        },
                                    ],
                                },
                            ]

                        elif not 'portfolios' in path:
                            start_date_string = from_date
                            end_date_string = to_date
                            
                            # Find objects between from_date (start_date_string) and end_date(end_date_string)
                            query['$and'] = [
                                {
                                    'startDate': { '$lte': end_date_string },
                                },
                                { 
                                    '$or': [
                                        {
                                            'endDate': { '$exists': False },
                                        },
                                        {
                                            '$and': [
                                                {
                                                    'endDate': { '$gte': start_date_string },
                                                },
                                            ],
                                        },
                                    ],
                                },
                            ]

                if device_types:
                    device_types = device_types.split(Constants.COMMA)
                    query['deviceTypes'] = {
                        '$in': device_types,
                    }
                
                if expression_type:
                    expression_type_pattern = re.compile(
                        expression_type,
                        re.IGNORECASE,
                    )
                    query['expressionType'] = expression_type_pattern
                
                if geo_locations:
                    geo_locations = geo_locations.split(Constants.COMMA)
                    query['id'] = {
                        '$in': geo_locations,
                    }
                
                if key and model:
                    query[f'{model}Id'] = key

                if keyword_text:
                    keyword_pattern = re.compile(
                        keyword_text,
                        re.IGNORECASE,
                    )
                    query['keywordText'] = keyword_pattern

                if line_item_ids:
                    line_item_ids = line_item_ids.split(Constants.COMMA)
                    query['lineItemId'] = {
                        '$in': line_item_ids,
                    }

                if line_item_type:
                    if line_item_type == 'undefined':
                        line_item_type = 'STANDARD_DISPLAY'

                    query['lineItemType'] = line_item_type
                
                if name or q:  # query is sent as q or name
                    or_query = query.get('$or', [])
                    
                    if name:
                        escaped_name = re.escape(name)
                        name_pattern = re.compile(
                            escaped_name,
                            re.IGNORECASE,
                        )

                        or_query.extend([
                            { 'name': { '$regex': name_pattern }, },
                        ])

                    if q:
                        escaped_query = re.escape(q)
                        q_pattern = re.compile(
                            escaped_query,
                            re.IGNORECASE,
                        )
                        or_query.extend([
                            { 'name': { '$regex': q_pattern }, },
                        ])


                    query['$or'] = or_query

                if order_ids:
                    order_ids = order_ids.split(Constants.COMMA)
                    query['orderId'] = {
                        '$in': order_ids,
                    }
                
                if portfolio_ids:
                    portfolio_ids = portfolio_ids.split(Constants.COMMA)
                    portfolio_ids = [portfolio_id for portfolio_id in portfolio_ids]
                    query = {
                        '_path': path,
                        'portfolioId': { '$in': portfolio_ids },
                    }

                
                if state:
                    query['state'] = {
                        '$in': state.split(Constants.COMMA),
                    }

                if supply_source_type:
                    query['supplySourceType'] = supply_source_type

                print(query)

                return list(
                    database[advertiser_id].find(
                        query,
                        { '_id': 0, '_path': 0 },
                    )
                )

            # Request using the external API
            value = await func(self, *args, **kwargs)
            
            items = value

            if isinstance(items, requests.Response):
                items = items.json()

            key = _key(request.url.path)
             
            # Update the cache with the response from the API
            if is_many:
                # DSP
                if dsp_advertiser_id:
                    log.info(
                        f'Switching advertiser_id from {advertiser_id} to {dsp_advertiser_id}'
                    )
                    advertiser_id = dsp_advertiser_id

                if isinstance(items, dict):
                    items = items.get('response', [])

                    log.info(
                        f'Caching DSP data {items}...'
                    )

                    for item in items:
                        log.info(
                            f'Caching DSP data {item} using {request.url.path} and advertiser_id {advertiser_id}...'
                        )
                        
                        item.update({'_path': request.url.path})
                        if isinstance(key, list):
                            database[advertiser_id].replace_one(
                                { '$and': [ { key[0]: str(item.get(key[0])), key[1]: str(item.get(key[1])) }, { '_path': request.url.path }, ] },
                                item,
                                upsert=True,
                            )
                        else:
                            database[advertiser_id].replace_one(
                                { '$and': [ { key: str(item.get(key)) }, { '_path': request.url.path }, ] },
                                item,
                                upsert=True,
                            )

                        log.info(
                            f'Cached DSP data {item} using {request.url.path} and advertiser_id {advertiser_id}'
                        )

                    log.info(
                        f'Cached DSP data'
                    )

                    return value
                
                # Sponsored Ads
                if isinstance(items, list) and len(items):
                    items = cache_utility.stringify_ids(items, request.url.path)
                    
                    for item in items:
                        
                        item.update({'_path': request.url.path})
                        database[advertiser_id].replace_one(
                            { '$and': [ { key: item.get(key) }, { '_path': request.url.path }, ] },
                            item,
                            upsert=True,
                        )
            elif items:  # `items` is actually a single dict, not a list at this point
                items = cache_utility.stringify_id(items, request.url.path)
                
                items['_path'] = request.url.path
                database[advertiser_id].replace_one(
                    { '$and': [ { key: items.get(key) }, { '_path': request.url.path }, ] },
                    items,
                    upsert=True,
                )
            
            return value
        
        return inner
    
    return wrapper


# TODO(declan.ryan@getvisibly.com): Remove support for Redis
def redis_cache(key_builder: Callable = None, is_json: bool = True, is_many: bool = True, ignore: bool = False):
    def wrapper(func):
        @wraps(func)
        async def inner(*args, **kwargs):
            nonlocal key_builder

            wrapped_kwargs = kwargs.copy()
            advertiser_id = wrapped_kwargs.pop(Constants.ADVERTISER_ID, None)
            request = wrapped_kwargs.pop(Constants.REQUEST, None)
            response = wrapped_kwargs.pop(Constants.RESPONSE, None)
            
            if (request and request.headers.get(Constants.CACHE_CONTROL)) or ignore == Constants.NO_STORE:
                return await func(*args, **kwargs)

            key_builder = key_builder or redis_service.key_builder

            key = key_builder(
                func, advertiser_id, request=request, args=args, kwargs=wrapped_kwargs,
            )

            if request.method != Constants.GET:
                return await func(request, *args, **kwargs)

            if_none_match = request.headers.get(
                Constants.IF_NONE_MATCH,
            )
            
            value = redis_service.get_json(key, is_many)
            if not is_json:
                value = redis_service.get(key)

            ttl = redis_service.ttl(key)

            if value and response:
                response.headers[Constants.CACHE_CONTROL] = f'{Constants.MAX_AGE}={ttl}'
                etag = f'W/{hash(str(value))}'

                if if_none_match == etag:
                    response.status_code = HTTP_304_NOT_MODIFIED
                    return response

                response.headers[Constants.ETAG] = etag
                return value

            if value:
                return value

            value = await func(*args, **kwargs)
            
            if is_json:
                redis_service.set_json(key, value, is_many)
            else:
                redis_service.set(key, value)

            return value
        
        return inner
    
    return wrapper


def _key(path):
    if 'ad_groups' in path:
        return 'adGroupId'
    elif Constants.CAMPAIGNS in path:
        return 'campaignId'
    elif Constants.KEYWORDS in path:
        return 'keywordId'
    elif Constants.PORTFOLIOS in path:
        return 'portfolioId'
    elif 'product_ads' in path:
        return 'adId'
    elif Constants.TARGETS in path:
        return 'targetId'
    elif Constants.ADVERTISERS in path:
        return 'advertiserId'
    elif Constants.ORDERS in path:
        return 'orderId'
    elif 'line_items' in path:
        return 'lineItemId'
    elif Constants.CREATIVES in path:
        return 'creativeId'
    elif 'line_item_creative_association' in path:
        return ['lineItemId', 'creativeId']
