import asyncio
import logging
import os
import sys
import time
import traceback

from amazon_api.resources.aa.client import Client
from fastapi import Request

import click
import requests

from server.core.constants import Constants
from server.delegates.aa_delegate import AADelegate
from server.dependencies import Interface
from server.services.aws_service import AWSService
from server.utilities.aa_utility import AAUtility


log = AWSService().log_service
documentdb = AWSService().docdb_service

DSP_BATCH_SIZE=100
DSP_TOTAL_RESULTS_LIMIT=100_000
LINE_ITEM_CREATIVE_ASSOCIATION_MAX_PAGE_SIZE=20
MAX_PAGE_SIZE=100
RETRY_AFTER='Retry-After'
RETRY_DELAY=10
TOO_MANY_REQUESTS_STATUS_CODE=429


class CacheContext:

    def __init__(self):
        pass


@click.group()
@click.pass_context
def cache(ctx):
    ctx.obj = CacheContext()


@cache.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.argument('region')
@click.option('--entity_id', '-e', default=None, required=True)
@click.option('--advertiser_id', '-a', default=None, required=False)
@click.pass_obj
def dsp(obj, region, entity_id, advertiser_id):
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(
            _cache_dsp(region, entity_id, advertiser_id),
        )
    finally:
        loop.close()


@cache.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.argument('region')
@click.option('--advertiser_id', '-a', default=None, required=False)
@click.pass_obj
def sa(obj, region, advertiser_id):
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(
            _cache_sa(region, advertiser_id),
        )
    finally:
        loop.close()


async def _cache_dsp(region, entity_id, advertiser_id):
    database = documentdb.client.amazon
    start_time = time.time()
    
    if advertiser_id == Constants.EMPTY_STRING:
        advertiser_id = None
    
    advertiser_ids = [advertiser_id]
    if advertiser_id is None:
        advertiser_ids = await _dsp_advertiser_ids(region, entity_id)
    
    log.info(f'Caching {len(advertiser_ids)} advertiser in {entity_id}...')
    for advertiser_id in advertiser_ids:
        log.info(f'Caching {entity_id}/{advertiser_id}...')
        
        log.info(f'Caching DSP orders for {entity_id}/{advertiser_id}...')
        await _orders(entity_id, advertiser_id, region)
        log.info(f'Cached DSP orders for {entity_id}/{advertiser_id}')
        
        log.info(f'Caching DSP line items for {entity_id}/{advertiser_id}...')
        items = database[entity_id].find(
            { '_path': '/api/v1/amazon/aa/dsp/orders' },
            { 'orderId': 1, '_id': 0 },
        )
        order_ids = [item.get('orderId') for item in items]
        await _line_items(entity_id, order_ids, region)
        log.info(f'Cached DSP line items for {entity_id}/{advertiser_id}')
        
        items = database[entity_id].find(
            { '_path': '/api/v1/amazon/aa/dsp/line_items', },
            { 'lineItemId': 1, '_id': 0 },
        )
        line_item_ids = [item.get('lineItemId') for item in items]
        log.info(f'Caching DSP line item creative associations for {entity_id}/{advertiser_id}...')
        await _line_item_creative_associations(entity_id, line_item_ids, region)
        log.info(f'Cached DSP line item creative associations for {entity_id}/{advertiser_id}')
        
        log.info(f'Caching DSP creatives for {entity_id}/{advertiser_id}...')
        await _creatives(entity_id, advertiser_id, region)
        log.info(f'Cached DSP creatives for {entity_id}/{advertiser_id}')
        
        duration = time.time() - start_time

        log.info(f'{round(duration/60)} minutes | Cached DSP for {entity_id}/{advertiser_id}')

    duration = time.time() - start_time

    log.info(f'{round(duration/60)} minutes | Cached {len(advertiser_ids)} advertisers in {entity_id}')
    
    
async def _cache_sa(region, advertiser_id):
    start_time = time.time()
    
    if advertiser_id == Constants.EMPTY_STRING:
        advertiser_id = None
    
    advertiser_ids = [advertiser_id]
    if advertiser_id is None:
        advertiser_ids = await _sa_advertiser_ids(region)
    
    log.info(f'Caching {len(advertiser_ids)} advertiser')
    for advertiser_id in advertiser_ids:
        log.info(f'Caching {advertiser_id}...')
        log.info(f'Caching portfolios for {advertiser_id}...')
        await _portfolios(advertiser_id, 'portfolios', region)
        log.info(f'Cached portfolios for {advertiser_id}')
    
    apis = [
        # Constants.SPONSORED_BRANDS,
        # Constants.SPONSORED_DISPLAY,
        # Constants.SPONSORED_PRODUCTS,
    ]
    if region == 'test':
        apis = [
            Constants.SPONSORED_DISPLAY,
            Constants.SPONSORED_PRODUCTS,
        ]

    log.info(f'Caching {len(advertiser_ids)} advertiser(s)')
    for advertiser_id in advertiser_ids:
        log.info(f'Caching {advertiser_id}...')
        # log.info(f'Caching portfolios for {advertiser_id}...')
        # await _portfolios(advertiser_id, 'portfolios', region)
        # log.info(f'Cached portfolios for {advertiser_id}')
        
        for api in apis:
            log.info(f'Caching {api} campaigns for {advertiser_id}...')
            await _campaigns(advertiser_id, api, region)
            log.info(f'Cached {api} campaigns for {advertiser_id}')
            log.info(f'Caching {api} ad groups for {advertiser_id}...')
            await _ad_groups(advertiser_id, api, region)
            log.info(f'Cached {api} ad groups for {advertiser_id}')
            log.info(f'Caching {api} keywords for {advertiser_id}...')
            await _keywords(advertiser_id, api, region)
            log.info(f'Cached {api} keywords for {advertiser_id}')
            log.info(f'Caching {api} product ads for {advertiser_id}...')
            await _product_ads(advertiser_id, api, region)
            log.info(f'Cached {api} product ads for {advertiser_id}')
            log.info(f'Caching {api} targets for {advertiser_id}...')
            await _targets(advertiser_id, api, region)
            log.info(f'Cached {api} targets for {advertiser_id}')

            duration = time.time() - start_time

            log.info(f'{round(duration/60)} minutes | Cached {api} for {advertiser_id}')
        
        duration = time.time() - start_time

        log.info(f'{round(duration/60)} minutes | Cached {advertiser_id}')

    duration = time.time() - start_time

    log.info(f'{round(duration/60)} minutes | Cached {len(advertiser_ids)} advertisers')


# DSP

async def _dsp_advertiser_ids(entity_id, region):
    api = Constants.DSP
    aa_utility = AAUtility()
    aws_service = AWSService()

    ssm_service = aws_service.ssm_service
    sts_service = aws_service.sts_service
    sts_service.assume_role(
        ssm_service.shared_parameter_store_role_arn,
        1*60*60,
    )
    aa_ssm_service = AWSService.AASSMService(
        sts_service,
        ssm_service.shared_parameter_store_role_arn,
    )
    
    version = aa_utility.version_for_api(
        api,
    )

    aa_delegate = AADelegate(
        api,
        region,
        version,
        ssm_service,
        aa_ssm_service,
    )

    client = Client(aa_delegate)

    klass = aa_utility.advertiser_interface_klass()
    interface = Interface(client, klass)

    response = {'response': [0]}
    start_index, totalResults = 0, DSP_TOTAL_RESULTS_LIMIT
    while start_index < totalResults and len(response.get('response', [])):
        log.info(f'Requesting advertisers {start_index} - {start_index + MAX_PAGE_SIZE}...')
        request = Request(
            scope={
                'headers': [],
                'method': 'GET',
                'scheme': 'http',
                'server': ('barcelona.getvisibly.com', 443,),
                'path': f'/api/v1/amazon/aa/{api}/advertisers',
                'query_string': f'startIndex={start_index}&count={MAX_PAGE_SIZE}'.encode(),
                'type': 'http',
            }
        )
        response = await interface.index(
            advertiser_id=entity_id,
            request=request,
        )

        while response.status_code == TOO_MANY_REQUESTS_STATUS_CODE:
            delay = response.headers.get(
                RETRY_AFTER,
                RETRY_DELAY,
            )
            log.info(
                f'Requesting advertisers after {delay} seconds...'
            )
            time.sleep(int(delay))
            response = await interface.index(
                advertiser_id=entity_id,
                request=request,
            )

        response = response.json()
        log.info(response)
        log.info(f'Requested advertisers {start_index} - {start_index + MAX_PAGE_SIZE}')
        start_index += MAX_PAGE_SIZE
        total_results = response.get('totalResults', 0)

        
async def _orders(entity_id, advertiser_id, region):
    api = Constants.DSP
    aa_utility = AAUtility()
    aws_service = AWSService()

    ssm_service = aws_service.ssm_service
    sts_service = aws_service.sts_service
    sts_service.assume_role(
        ssm_service.shared_parameter_store_role_arn,
        1*60*60,
    )
    aa_ssm_service = AWSService.AASSMService(
        sts_service,
        ssm_service.shared_parameter_store_role_arn,
    )
    
    version = aa_utility.version_for_api(
        api,
    )

    aa_delegate = AADelegate(
        api,
        region,
        version,
        ssm_service,
        aa_ssm_service,
    )

    client = Client(aa_delegate)

    klass = aa_utility.order_interface_klass()
    interface = Interface(client, klass)

    response = {'response': [0]}
    start_index, totalResults = 0, DSP_TOTAL_RESULTS_LIMIT
    while start_index < totalResults and len(response.get('response', [])):
        log.info(f'Requesting orders {start_index} - {start_index + MAX_PAGE_SIZE}...')
        request = Request(
            scope={
                'headers': [],
                'method': 'GET',
                'scheme': 'http',
                'server': ('barcelona.getvisibly.com', 443,),
                'path': f'/api/v1/amazon/aa/{api}/orders',
                'query_string': f'advertiserIdFilter={advertiser_id}&startIndex={start_index}&count={MAX_PAGE_SIZE}'.encode(),
                'type': 'http',
            }
        )
        response = await interface.index(
            advertiser_id=entity_id,
            request=request,
        )
        
        while response.status_code == TOO_MANY_REQUESTS_STATUS_CODE:
            delay = response.headers.get(
                RETRY_AFTER,
                RETRY_DELAY,
            )
            log.info(
                f'Requesting orders after {delay} seconds...'
            )
            time.sleep(int(delay))
            response = await interface.index(
                advertiser_id=entity_id,
                request=request,
            )

        response = response.json()
        log.info(f'Requested orders {start_index} - {start_index + MAX_PAGE_SIZE}')
        start_index += MAX_PAGE_SIZE
        total_results = response.get('totalResults', 0)


async def _line_items(entity_id, order_ids, region):
    api = Constants.DSP
    aa_utility = AAUtility()
    aws_service = AWSService()

    ssm_service = aws_service.ssm_service
    sts_service = aws_service.sts_service
    sts_service.assume_role(
        ssm_service.shared_parameter_store_role_arn,
        1*60*60,
    )
    aa_ssm_service = AWSService.AASSMService(
        sts_service,
        ssm_service.shared_parameter_store_role_arn,
    )
    
    version = aa_utility.version_for_api(
        api,
    )

    aa_delegate = AADelegate(
        api,
        region,
        version,
        ssm_service,
        aa_ssm_service,
    )

    client = Client(aa_delegate)

    klass = aa_utility.line_item_interface_klass()
    interface = Interface(client, klass)

    for order_id_batch in batches(order_ids, DSP_BATCH_SIZE):
        
        order_id_filter = Constants.COMMA.join(order_id_batch)
        response = {'response': [0]}
        start_index, totalResults = 0, DSP_TOTAL_RESULTS_LIMIT
    
        while start_index < totalResults and len(response.get('response', [])):
            log.info(f'Requesting line items {start_index} - {start_index + MAX_PAGE_SIZE}...')
            request = Request(
                scope={
                    'headers': [],
                    'method': 'GET',
                    'scheme': 'http',
                    'server': ('barcelona.getvisibly.com', 443,),
                    'path': f'/api/v1/amazon/aa/{api}/line_items',
                    'query_string': f'orderIdFilter={order_id_filter}&startIndex={start_index}&count={MAX_PAGE_SIZE}'.encode(),
                    'type': 'http',
                }
            )
            response = await interface.index(
                advertiser_id=entity_id,
                request=request,
            )
            
            while response.status_code == TOO_MANY_REQUESTS_STATUS_CODE:
                delay = response.headers.get(
                    RETRY_AFTER,
                    RETRY_DELAY,
                )
                log.info(
                    f'Requesting line items after {delay} seconds...'
                )
                time.sleep(int(delay))
                response = await interface.index(
                    advertiser_id=entity_id,
                    request=request,
                )

            response = response.json()
            log.info(f'Requested line items {start_index} - {start_index + MAX_PAGE_SIZE}')
            start_index += MAX_PAGE_SIZE
            total_results = response.get('totalResults', 0)


async def _line_item_creative_associations(entity_id, line_item_ids, region):
    api = Constants.DSP
    aa_utility = AAUtility()
    aws_service = AWSService()

    ssm_service = aws_service.ssm_service
    sts_service = aws_service.sts_service
    sts_service.assume_role(
        ssm_service.shared_parameter_store_role_arn,
        1*60*60,
    )
    aa_ssm_service = AWSService.AASSMService(
        sts_service,
        ssm_service.shared_parameter_store_role_arn,
    )
    
    version = aa_utility.version_for_api(
        api,
    )

    aa_delegate = AADelegate(
        api,
        region,
        version,
        ssm_service,
        aa_ssm_service,
    )

    client = Client(aa_delegate)

    klass = aa_utility.line_item_interface_klass()
    interface = Interface(client, klass)

    for line_item_id_batch in batches(line_item_ids, DSP_BATCH_SIZE):
        
        line_item_id_filter = Constants.COMMA.join(line_item_id_batch)
        response = {'response': [0]}
        start_index, totalResults = 0, DSP_TOTAL_RESULTS_LIMIT

        while start_index < totalResults and len(response.get('response', [])):
            log.info(f'Requesting line item creative assocations {start_index} - {start_index + LINE_ITEM_CREATIVE_ASSOCIATION_MAX_PAGE_SIZE}...')
            request = Request(
                scope={
                    'headers': [],
                    'method': 'GET',
                    'scheme': 'http',
                    'server': ('barcelona.getvisibly.com', 443,),
                    'path': f'/api/v1/amazon/aa/{api}/line_item_creative_associations',
                    'query_string': f'lineItemIdFilter={line_item_id_filter}&startIndex={start_index}&count={LINE_ITEM_CREATIVE_ASSOCIATION_MAX_PAGE_SIZE}'.encode(),
                    'type': 'http',
                }
            )
            response = await interface.index_creative_association(
                advertiser_id=entity_id,
                request=request,
            )
            
            while response.status_code == TOO_MANY_REQUESTS_STATUS_CODE:
                delay = response.headers.get(
                    RETRY_AFTER,
                    RETRY_DELAY,
                )
                log.info(
                    f'Requesting line item creative associations after {delay} seconds...'
                )
                time.sleep(int(delay))
                response = await interface.index_creative_association(
                    advertiser_id=entity_id,
                    request=request,
                )

            response = response.json()
            log.info(f'Requested line item creative assocations {start_index} - {start_index + LINE_ITEM_CREATIVE_ASSOCIATION_MAX_PAGE_SIZE}')
            start_index += LINE_ITEM_CREATIVE_ASSOCIATION_MAX_PAGE_SIZE
            total_results = response.get('totalResults', 0)


async def _creatives(entity_id, advertiser_id, region):
    api = Constants.DSP
    aa_utility = AAUtility()
    aws_service = AWSService()

    ssm_service = aws_service.ssm_service
    sts_service = aws_service.sts_service
    sts_service.assume_role(
        ssm_service.shared_parameter_store_role_arn,
        1*60*60,
    )
    aa_ssm_service = AWSService.AASSMService(
        sts_service,
        ssm_service.shared_parameter_store_role_arn,
    )
    
    version = aa_utility.version_for_api(
        api,
    )

    aa_delegate = AADelegate(
        api,
        region,
        version,
        ssm_service,
        aa_ssm_service,
    )

    client = Client(aa_delegate)

    klass = aa_utility.creative_interface_klass()
    interface = Interface(client, klass)

    response = {'response': [0]}
    start_index, totalResults = 0, DSP_TOTAL_RESULTS_LIMIT
    while start_index < totalResults and len(response.get('response', [])):
        log.info(f'Requesting creatives {start_index} - {start_index + MAX_PAGE_SIZE}...')
        request = Request(
            scope={
                'headers': [],
                'method': 'GET',
                'scheme': 'http',
                'server': ('barcelona.getvisibly.com', 443,),
                'path': f'/api/v1/amazon/aa/{api}/creatives',
                'query_string': f'advertiserIdFilter={advertiser_id}&startIndex={start_index}&count={MAX_PAGE_SIZE}'.encode(),
                'type': 'http',
            }
        )
        response = await interface.index(
            advertiser_id=entity_id,
            request=request,
        )
        
        while response.status_code == TOO_MANY_REQUESTS_STATUS_CODE:
            delay = response.headers.get(
                RETRY_AFTER,
                RETRY_DELAY,
            )
            log.info(
                f'Requesting creatives after {delay} seconds...'
            )
            time.sleep(int(delay))
            response = await interface.index(
                advertiser_id=entity_id,
                request=request,
            )

        response = response.json()
        log.info(f'Requested creatives {start_index} - {start_index + MAX_PAGE_SIZE}')
        start_index += MAX_PAGE_SIZE
        total_results = response.get('totalResults', 0)


# Sponsored Ads

async def _sa_advertiser_ids(region):
    aa_utility = AAUtility()
    aws_service = AWSService()
    
    ssm_service = aws_service.ssm_service
    sts_service = aws_service.sts_service
    sts_service.assume_role(
        ssm_service.shared_parameter_store_role_arn,
        1*60*60,
    )
    aa_ssm_service = AWSService.AASSMService(
        sts_service,
        ssm_service.shared_parameter_store_role_arn,
    )
    
    version = aa_utility.version_for_api(
        Constants.PROFILES,
    )
    aa_delegate = AADelegate(
        Constants.PROFILES,
        region,
        version,
        ssm_service,
        aa_ssm_service,
    )

    client = Client(aa_delegate)

    klass = aa_utility.profile_interface_klass()
    interface = Interface(client, klass)

    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('barcelona.getvisibly.com', 443,),
            'path': f'/api/v1/amazon/aa/profiles',
            'query_string': b'',
            'type': 'http',
        }
    )
    
    response = await interface.index(
        advertiser_id=None,
        request=request,
    )
    response = response.json()

    advertiser_ids = [
        str(advertiser.get('profileId')) for advertiser in response
    ]

    return advertiser_ids
    

async def _ad_groups(advertiser_id, api, region):
    aa_utility = AAUtility()
    aws_service = AWSService()

    ssm_service = aws_service.ssm_service
    sts_service = aws_service.sts_service
    sts_service.assume_role(
        ssm_service.shared_parameter_store_role_arn,
        1*60*60,
    )
    aa_ssm_service = AWSService.AASSMService(
        sts_service,
        ssm_service.shared_parameter_store_role_arn,
    )
    
    version = aa_utility.version_for_api(
        api,
    )

    aa_delegate = AADelegate(
        api,
        region,
        version,
        ssm_service,
        aa_ssm_service,
    )

    client = Client(aa_delegate)

    klass = aa_utility.ad_group_interface_klass(
        api,
    )
    interface = Interface(client, klass)

    response = [0]
    start_index = 0
    while start_index < 20000 and len(response) > 0:
        log.info(f'Requesting {api} ad groups {start_index} - {start_index + MAX_PAGE_SIZE}...')
        request = Request(
            scope={
                'headers': [],
                'method': 'GET',
                'scheme': 'http',
                'server': ('barcelona.getvisibly.com', 443,),
                'path': f'/api/v1/amazon/aa/{api}/ad_groups',
                'query_string': f'startIndex={start_index}&count={MAX_PAGE_SIZE}'.encode(),
                'type': 'http',
            }
        )
        response = await interface.index(
            advertiser_id=advertiser_id,
            request=request,
        )

        response = response.json()
        log.info(f'Requested {api} ad groups {start_index} - {start_index + MAX_PAGE_SIZE}')
        start_index += MAX_PAGE_SIZE


async def _campaigns(advertiser_id, api, region):
    aa_utility = AAUtility()
    aws_service = AWSService()

    ssm_service = aws_service.ssm_service
    sts_service = aws_service.sts_service
    sts_service.assume_role(
        ssm_service.shared_parameter_store_role_arn,
        1*60*60,
    )
    aa_ssm_service = AWSService.AASSMService(
        sts_service,
        ssm_service.shared_parameter_store_role_arn,
    )
    
    version = aa_utility.version_for_api(
        api,
    )

    aa_delegate = AADelegate(
        api,
        region,
        version,
        ssm_service,
        aa_ssm_service,
    )

    client = Client(aa_delegate)

    klass = aa_utility.campaign_interface_klass(
        api,
    )
    interface = Interface(client, klass)

    response = [0]
    start_index = 0
    while start_index < 20000 and len(response):
        log.info(f'Requesting {api} campaigns {start_index} - {start_index + MAX_PAGE_SIZE}...')
        request = Request(
            scope={
                'headers': [],
                'method': 'GET',
                'scheme': 'http',
                'server': ('barcelona.getvisibly.com', 443,),
                'path': f'/api/v1/amazon/aa/{api}/campaigns',
                'query_string': f'startIndex={start_index}&count={MAX_PAGE_SIZE}'.encode(),
                'type': 'http',
            }
        )
        response = await interface.index(
            advertiser_id=advertiser_id,
            request=request,
        )
        response = response.json()
        log.info(f'Requested {api} campaigns {start_index} - {start_index + MAX_PAGE_SIZE}')
        start_index += MAX_PAGE_SIZE


async def _keywords(advertiser_id, api, region):
    if api == Constants.SPONSORED_DISPLAY: return

    aa_utility = AAUtility()
    aws_service = AWSService()

    ssm_service = aws_service.ssm_service
    sts_service = aws_service.sts_service
    sts_service.assume_role(
        ssm_service.shared_parameter_store_role_arn,
        1*60*60,
    )
    aa_ssm_service = AWSService.AASSMService(
        sts_service,
        ssm_service.shared_parameter_store_role_arn,
    )
    
    version = aa_utility.version_for_api(
        api,
    )

    aa_delegate = AADelegate(
        api,
        region,
        version,
        ssm_service,
        aa_ssm_service,
    )

    client = Client(aa_delegate)

    klass = aa_utility.keyword_interface_klass(
        api,
    )
    interface = Interface(client, klass)

    response = [0]
    start_index = 0
    while start_index < 20000 and len(response) > 0:
        log.info(f'Requesting {api} keywords {start_index} - {start_index + MAX_PAGE_SIZE}...')
        request = Request(
            scope={
                'headers': [],
                'method': 'GET',
                'scheme': 'http',
                'server': ('barcelona.getvisibly.com', 443,),
                'path': f'/api/v1/amazon/aa/{api}/keywords',
                'query_string': f'startIndex={start_index}&count={MAX_PAGE_SIZE}'.encode(),
                'type': 'http',
            }
        )
        response = await interface.index(
            advertiser_id=advertiser_id,
            request=request,
        )

        response = response.json()
        log.info(f'Requested {api} keywords {start_index} - {start_index + MAX_PAGE_SIZE}')
        start_index += MAX_PAGE_SIZE


async def _portfolios(advertiser_id, api, region):
    aa_utility = AAUtility()
    aws_service = AWSService()

    ssm_service = aws_service.ssm_service
    sts_service = aws_service.sts_service
    sts_service.assume_role(
        ssm_service.shared_parameter_store_role_arn,
        1*60*60,
    )
    aa_ssm_service = AWSService.AASSMService(
        sts_service,
        ssm_service.shared_parameter_store_role_arn,
    )
    
    version = aa_utility.version_for_api(
        api,
    )

    aa_delegate = AADelegate(
        api,
        region,
        version,
        ssm_service,
        aa_ssm_service,
    )

    client = Client(aa_delegate)

    klass = aa_utility.portfolio_interface_klass()
    interface = Interface(client, klass)
    
    log.info(f'Requesting portfolios with list...')
    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('barcelona.getvisibly.com', 443,),
            'path': f'/api/v1/amazon/aa/portfolios/list',
            'query_string': b'',
            'type': 'http',
        }
    )
    response = await interface.index(
        advertiser_id=advertiser_id,
        request=request,
    )

    log.info(f'Requesting portfolios without list...')
    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('barcelona.getvisibly.com', 443,),
            'path': f'/api/v1/amazon/aa/portfolios',
            'query_string': b'',
            'type': 'http',
        }
    )
    response = await interface.index(
        advertiser_id=advertiser_id,
        request=request,
    )

    log.info(f'Requested portfolios')

    log.info(f'Requesting portfolios with graph...')
    request = Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('barcelona.getvisibly.com', 443,),
            'path': f'/api/v1/amazon/aa/portfolios/graphs/index',
            'query_string': b'',
            'type': 'http',
        }
    )
    response = await interface.index(
        advertiser_id=advertiser_id,
        request=request,
    )
    log.info(response.json())

    log.info(f'Requested portfolios')


async def _product_ads(advertiser_id, api, region):
    if api == Constants.SPONSORED_BRANDS: return

    aa_utility = AAUtility()
    aws_service = AWSService()

    ssm_service = aws_service.ssm_service
    sts_service = aws_service.sts_service
    sts_service.assume_role(
        ssm_service.shared_parameter_store_role_arn,
        1*60*60,
    )
    aa_ssm_service = AWSService.AASSMService(
        sts_service,
        ssm_service.shared_parameter_store_role_arn,
    )
    
    version = aa_utility.version_for_api(
        api,
    )

    aa_delegate = AADelegate(
        api,
        region,
        version,
        ssm_service,
        aa_ssm_service,
    )

    client = Client(aa_delegate)

    klass = aa_utility.product_ad_interface_klass(
        api,
    )
    interface = Interface(client, klass)

    response = [0]
    start_index = 0
    while start_index < 20000 and len(response) > 0:
        log.info(f'Requesting {api} product ads {start_index} - {start_index + MAX_PAGE_SIZE}...')
        request = Request(
            scope={
                'headers': [],
                'method': 'GET',
                'scheme': 'http',
                'server': ('barcelona.getvisibly.com', 443,),
                'path': f'/api/v1/amazon/aa/{api}/product_ads',
                'query_string': f'startIndex={start_index}&count={MAX_PAGE_SIZE}'.encode(),
                'type': 'http',
            }
        )
        response = await interface.index(
            advertiser_id=advertiser_id,
            request=request,
        )

        response = response.json()
        log.info(f'Requested {api} product ads {start_index} - {start_index + MAX_PAGE_SIZE}')
        start_index += MAX_PAGE_SIZE


async def _targets(advertiser_id, api, region):
    aa_utility = AAUtility()
    aws_service = AWSService()

    ssm_service = aws_service.ssm_service
    sts_service = aws_service.sts_service
    sts_service.assume_role(
        ssm_service.shared_parameter_store_role_arn,
        1*60*60,
    )
    aa_ssm_service = AWSService.AASSMService(
        sts_service,
        ssm_service.shared_parameter_store_role_arn,
    )
    
    version = aa_utility.version_for_api(
        api,
    )

    aa_delegate = AADelegate(
        api,
        region,
        version,
        ssm_service,
        aa_ssm_service,
    )

    client = Client(aa_delegate)

    klass = aa_utility.target_interface_klass(
        api,
    )
    interface = Interface(client, klass)

    if api == Constants.SPONSORED_BRANDS:
        first_request = True
        next_token = None
        while next_token is not None or first_request:            
            log.info(f'Requesting {api} targets...')
            request = Request(
                scope={
                    'headers': [],
                    'method': 'POST',
                    'scheme': 'http',
                    'server': ('barcelona.getvisibly.com', 443,),
                    'path': f'/api/v1/amazon/aa/{api}/targets',
                    'query_string': b'',
                    'type': 'http',
                }
            )
            response = await interface.index_create(
                advertiser_id=advertiser_id,
                data={
                    'filters': [
                        {
                            'filterType': 'TARGETING_STATE',
                            'values': [
                                'archived',
                                'enabled',
                                'paused',
                            ]
                        }
                    ],
                    'maxResults': 5000,
                    'nextToken': next_token,
                },
                request=request,
            )
            
            response = response.json()
            next_token = response.get('nextToken')
            first_request = False
            log.info(f'Requested {api} targets')
    else:
        response = [0]
        start_index = 0
    
        while start_index < 20000 and len(response) > 0:
            log.info(f'Requesting {api} targets {start_index} - {start_index + MAX_PAGE_SIZE}...')
            request = Request(
                scope={
                    'headers': [],
                    'method': 'GET',
                    'scheme': 'http',
                    'server': ('barcelona.getvisibly.com', 443,),
                    'path': f'/api/v1/amazon/aa/{api}/targets',
                    'query_string': f'startIndex={start_index}&count={MAX_PAGE_SIZE}'.encode(),
                    'type': 'http',
                }
            )
            response = await interface.index(
                advertiser_id=advertiser_id,
                request=request,
            )

            response = response.json()
            log.info(f'Requested {api} targets {start_index} - {start_index + MAX_PAGE_SIZE}')
            start_index += MAX_PAGE_SIZE


def batches(items, n):
    for i in range(0, len(items), n):
        yield items[i:i + n]
