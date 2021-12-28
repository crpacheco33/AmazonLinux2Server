import json
import logging
import os
import sys
import traceback

import click
import requests

from server.core.constants import Constants
from server.resources.models.tag import Tag
from server.services.aws_service import AWSService


log = AWSService().log_service
log.handler = logging.StreamHandler(
    sys.stdout,
)

class TagContext:

    def __init__(self):
        pass


@click.group()
@click.pass_context
def tags(ctx):
    ctx.obj = TagContext()


@tags.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.option('--groups/--no-groups', default=False)
@click.pass_obj
def index(obj, groups):
    access_token = os.getenv('ACCESS_TOKEN')
    
    url = 'http://127.0.0.1:8080/api/v1/tags'
    if groups:
        url = f'http://127.0.0.1:8080/api/v1/tags/groups'
    
    try:
        response = requests.get(
            url,
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(response.json())
    except Exception as e:
        log.info(f'Tags were not indexed')
        traceback.print_exc(e)


@tags.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.pass_obj
def new(obj):
    access_token = os.getenv('ACCESS_TOKEN')
    
    try:
        response = requests.get(
            'http://127.0.0.1:8080/api/v1/tags/new',
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(response.json())
    except Exception as e:
        log.info(f'Tags were not indexed')
        traceback.print_exc(e)


@tags.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.option('--path', '-p', required=True, type=click.Path(exists=True))
@click.pass_obj
def create(obj, path):
    access_token = os.getenv('ACCESS_TOKEN')
    data, error = _from_json(
        path,
    )
    
    if error is not None:
        log.info('Insight data does not exist')
        exit(1)

    for datum in data:
        try:
            response = requests.post(
                'http://127.0.0.1:8080/api/v1/tags',
                json=datum,
                headers={
                    'Authorization': f'Bearer {access_token}',
                },
            )
            log.info(response.json())
            log.info(f'Tag {datum.get("name")} was created.')
        except Exception as e:
            log.info(f'Tag {datum.get("name")} was not created.')
            traceback.print_exc(e)


@tags.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.pass_obj
@click.option('--tag_id', '-t', required=True, type=str)
def edit(obj, tag_id):
    access_token = os.getenv('ACCESS_TOKEN')
    
    try:
        response = requests.get(
            f'http://127.0.0.1:8080/api/v1/tags/{tag_id}/edit',
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(response.json())
    except Exception as e:
        log.info(f'Tags were not indexed')
        traceback.print_exc(e)


@tags.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.option('--path', '-p', required=True, type=click.Path(exists=True))
@click.option('--tag_id', '-t', required=True, type=str)
@click.pass_obj
def update(obj, path, tag_id):
    access_token = os.getenv('ACCESS_TOKEN')
    data, error = _from_json(
        path,
    )
    
    if error is not None:
        log.info('Insight data does not exist')
        exit(1)

    try:
        response = requests.put(
            f'http://127.0.0.1:8080/api/v1/tags/{tag_id}',
            json=data,
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(response.json())
        log.info(f'Tag {data.get("name")} was updated.')
    except Exception as e:
        log.info(f'Tag {data.get("name")} was not updated.')
        traceback.print_exc(e)


def _from_json(file):
    result = None
    try:
        with open(file, 'rb') as readfile:
            result = json.load(readfile)
    except (IOError, TypeError) as e:
        return None, e

    return result, None
