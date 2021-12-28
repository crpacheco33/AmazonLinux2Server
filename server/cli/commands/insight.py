import json
import logging
import os
import sys
import traceback

import click
import requests

from server.core.constants import Constants
from server.resources.models.insight import Insight
from server.services.aws_service import AWSService


log = AWSService().log_service
log.handler = logging.StreamHandler(
    sys.stdout,
)

class InsightContext:

    def __init__(self):
        pass


@click.group()
@click.pass_context
def insights(ctx):
    ctx.obj = InsightContext()


@insights.command(
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
            f'http://127.0.0.1:8080/api/v1/insights/new',
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(response.json())
        log.info(f'Indexed insights/new')
    except Exception as e:
        log.info(f'Did not index insights/new')
        traceback.print_exc(e)


@insights.command(
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
            f'http://127.0.0.1:8080/api/v1/insights',
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(response.json())
        log.info(f'Indexed insights.')
    except Exception as e:
        log.info(f'Did not index insights.')
        traceback.print_exc(e)


@insights.command(
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

    try:
        response = requests.post(
            'http://127.0.0.1:8080/api/v1/insights',
            json=data,
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(f'Insight {data.get("description")} was created.')
    except Exception as e:
        log.info(f'Insight {data.get("description")} was not created.')
        traceback.print_exc(e)


@insights.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.argument('insight_id')
@click.pass_obj
def show(obj, insight_id):
    access_token = os.getenv('ACCESS_TOKEN')
    
    try:
        response = requests.get(
            f'http://127.0.0.1:8080/api/v1/insights/{insight_id}',
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(response.json())
        log.info(f'Showed insight.')
    except Exception as e:
        log.info(f'Did not show insight.')
        traceback.print_exc(e)


def _from_json(file):
    result = None
    try:
        with open(file, 'rb') as readfile:
            result = json.load(readfile)
    except (IOError, TypeError) as e:
        return None, e

    return result, None
