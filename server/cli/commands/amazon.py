import logging
import os
import sys
import traceback

from bson.objectid import ObjectId

import click
import pymongo
import requests

from server.core.constants import Constants
from server.services.aws_service import AWSService


log = AWSService().log_service
log.handler = logging.StreamHandler(
    sys.stdout,
)



class AmazonContext:

    def __init__(self):
        pass


@click.group()
@click.pass_context
def amazon(ctx):
    ctx.obj = AmazonContext()


@amazon.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.pass_obj
@click.option('--api', '-a', default='sp', required=False, type=str)
@click.option('--start_date', '-s', default='2021-04-01', required=False, type=str)
@click.option('--end_date', '-e', default='2021-06-30', required=False, type=str)
def dashboard(obj, api, start_date, end_date):
    access_token = os.getenv('ACCESS_TOKEN')

    try:
        response = requests.get(
            f'http://127.0.0.1:8080/api/v1/amazon/aa/{api}/graphs/index?from_date={start_date}&to_date={end_date}&interval=day',
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(response.json())
        log.info(f'{api} dashboard between {start_date} and {end_date} returned')
    except Exception as e:
        log.info(f'Did not return {api} dashboard')
        traceback.print_exc(e)

@amazon.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.pass_obj
@click.option('--start_date', '-s', required=False, type=str)
@click.option('--end_date', '-e', required=False, type=str)
def sb(obj, start_date, end_date):
    access_token = os.getenv('ACCESS_TOKEN')

    try:
        response = requests.get(
            f'http://127.0.0.1:8080/api/v1/amazon/aa/sb/campaigns?start_date={start_date}&end_date={end_date}',
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(response.json())
        log.info(f'Indexed SB campaigns between {start_date} and {end_date}')
    except Exception as e:
        log.info(f'Did not index SB campaigns')
        traceback.print_exc(e)


@amazon.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.pass_obj
@click.option('--start_date', '-s', required=False, type=str)
@click.option('--end_date', '-e', required=False, type=str)
def sd(obj, start_date, end_date):
    access_token = os.getenv('ACCESS_TOKEN')

    try:
        response = requests.get(
            f'http://127.0.0.1:8080/api/v1/amazon/aa/sd/campaigns?start_date={start_date}&end_date={end_date}',
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(response.json())
        log.info(f'Indexed SD campaigns between {start_date} and {end_date}')
    except Exception as e:
        log.info(f'Did not index SD campaigns')
        traceback.print_exc(e)

@amazon.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.pass_obj
@click.option('--start_date', '-s', required=False, type=str)
@click.option('--end_date', '-e', required=False, type=str)
def sp(obj, start_date, end_date):
    access_token = os.getenv('ACCESS_TOKEN')

    try:
        response = requests.get(
            f'http://127.0.0.1:8080/api/v1/amazon/aa/sp/campaigns?start_date={start_date}&end_date={end_date}',
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(response.json())
        log.info(f'Indexed SP campaigns between {start_date} and {end_date}')
    except Exception as e:
        log.info(f'Did not index SP campaigns')
        traceback.print_exc(e)
