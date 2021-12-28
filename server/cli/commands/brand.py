import json
import logging
import os
import sys
import traceback

from bson.objectid import ObjectId

import click
import pymongo
import requests

from server.core.constants import Constants
from server.resources.models.brand import Brand
from server.services.aws_service import AWSService


log = AWSService().log_service
log.handler = logging.StreamHandler(
    sys.stdout,
)


BRANDS = {
    'barcelona': [
        {
            "name": "Sandbox",
            "country": "US",
            "currency": "USD",
            "timezone": "America/Los_Angeles",
            "amazon": {
                "aa": {
                    "region": "test",
                    "sa": { "advertiser_id": "4093311673971939", "name": "Sandbox", "advertiser_type": "vendor" }
                }
            }
        },
        {
            "name": "Canon USA (Lasers)",
            "country": "US",
            "currency": "USD",
            "timezone": "America/Los_Angeles",
            "amazon": {
                "aa": {
                    "dsp": {
                        "advertiser_id": "2575637870401",
                        "entity_id": "1191625552155416",
                        "name": "Canon"
                    },
                    "sa": { "advertiser_id": "1110250468092771", "name": "Canon USA (Lasers)", "advertiser_type": "vendor" },
                    "region": "na"
                }
            }
        }
    ],
    'local': [
        {
            "name": "Sandbox",
            "country": "US",
            "currency": "USD",
            "timezone": "America/Los_Angeles",
            "amazon": {
                "aa": {
                    "region": "test",
                    "sa": { "advertiser_id": "4093311673971939", "name": "Sandbox", "advertiser_type": "vendor" }
                }
            }
        },
        {
            "name": "Canon USA (Lasers)",
            "country": "US",
            "currency": "USD",
            "timezone": "America/Los_Angeles",
            "amazon": {
                "aa": {
                    "dsp": {
                        "advertiser_id": "2575637870401",
                        "entity_id": "1191625552155416",
                        "name": "Canon"
                    },
                    "sa": { "advertiser_id": "1110250468092771", "name": "Canon USA (Lasers)", "advertiser_type": "vendor" },
                    "region": "na"
                }
            }
        }
    ],
}


class BrandContext:

    def __init__(self):
        pass


@click.group()
@click.pass_context
def brands(ctx):
    ctx.obj = BrandContext()


@brands.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.pass_obj
@click.option('--deployment', '-d', required=True)
def add(obj, deployment):
    aws_service = AWSService()
    docdb_service = aws_service.docdb_service
    client = docdb_service.client

    with client.start_session() as session:
        collection = client.visibly.brands

        indices = collection.index_information()
        if indices.get('name') is None:
            collection.create_index(
                [( 'name', pymongo.DESCENDING,)],
                unique=True,
            )

    for brand in BRANDS[deployment]:
        Brand.create(brand, client)


@brands.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.pass_obj
def index(obj):
    access_token = os.getenv('ACCESS_TOKEN')
    
    try:
        response = requests.get(
            f'http://127.0.0.1:8080/api/v1/brands',
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(response.json())
        log.info(f'Indexed brands.')
    except Exception as e:
        log.info(f'Did not index brands.')
        traceback.print_exc(e)


@brands.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.pass_obj
@click.option('--path', '-p', required=False, type=click.Path(exists=True))
def remove_users_from_all(obj, path):
    aws_service = AWSService()
    client = aws_service.docdb_service.client

    if path:
        with open(path) as read_file:
            users = json.loads(read_file.read())

        for user in users:
            user_id = user.get('id')

            log.info(
                f'Removing user {user_id} from brands...',
            )
            with client.start_session() as session:
                collection = client.visibly.brands
                collection.update_many(
                    {},
                    { '$pull': { 'users': ObjectId(user_id) } },
                )
            log.info(
                f'Removed user {user_id} from brands',
            )
    else:
        log.info('No path provided')
        exit(1)


@brands.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.pass_obj
@click.option('--path', '-p', required=False, type=click.Path(exists=True))
def remove_users_from_many(obj, path):
    aws_service = AWSService()
    client = aws_service.docdb_service.client

    if path:
        with open(path) as read_file:
            users = json.loads(read_file.read())

        for user in users:
            user_id = user.get('id')
            brands = user.get('brands')

            log.info(
                f'Removing user {user_id} from {len(brands)} brands...',
            )

            user_id = ObjectId(user_id)

            for brand in brands:
                log.info(
                    f'Removing user {user_id} from brand {brand}...',
                )

                brand_id = ObjectId(brand)

                Brand.remove_user(
                    brand_id,
                    user_id,
                    client,
                )

                log.info(
                    f'Removed user {user_id} from brand {brand}',
                )

            log.info(
                f'Removed user {user_id} from {len(brands)} brands',
            )
    else:
        log.info('No path provided')
        exit(1)


@brands.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.pass_obj
@click.option('--brand', '-b', required=True, type=str)
def show(obj, brand):
    access_token = os.getenv('ACCESS_TOKEN')
    
    aws_service = AWSService()
    docdb_service = aws_service.docdb_service
    client = docdb_service.client

    try:
        response = requests.get(
            f'http://127.0.0.1:8080/api/v1/brands/{brand}',
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(response.json())
        log.info(f'Showed {brand}.')
    except Exception as e:
        log.info(f'Did not show brand {brand}.')
        traceback.print_exc(e)


@brands.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.pass_obj
@click.option('--brand', '-b', required=True, type=str)
def switch(obj, brand):
    access_token = os.getenv('ACCESS_TOKEN')
    
    aws_service = AWSService()
    docdb_service = DocumentDBService(aws_service.ssm_service)
    client = docdb_service.client

    brand = Brand.find_by_name(
        brand,
        client,
    )

    try:
        response = requests.put(
            f'http://127.0.0.1:8080/api/v1/brands/{str(brand._id)}',
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(f'Switched to brand {brand}.')
    except Exception as e:
        log.info(f'Did not switch to brand {brand}.')
        traceback.print_exc(e)
