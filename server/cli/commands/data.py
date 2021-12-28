import json
import logging
import os
import sys
import traceback

import click
import requests

from server.core.constants import Constants
from server.services.aws_service import AWSService
from server.services.data_service import DataService


log = AWSService().log_service
log.handler = logging.StreamHandler(
    sys.stdout,
)

class DataContext:

    def __init__(self):
        pass


@click.group()
@click.pass_context
def data(ctx):
    ctx.obj = DataContext()


@data.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.argument('method')
@click.pass_obj
def tags(obj, method):
    aws_service = AWSService()
    es_service = AWSService().es_service
    es_service.domain = AWSService().ssm_service.amazon_advertising_elasticsearch_domain

    data_service = DataService(es_service.es_service)

    if method == 'dsp_and_sa_tags':
        results = data_service.dsp_and_sa_tags(
            [90766455496743,11166462748649],
            [2676008490401,5061089110801],
            '2021-04-01',
            '2021-06-30',
            'day',
            'AM,BP,CQ,DC,UZ,XM',
            []
        )
    elif method == 'dsp_tags':
        results = data_service.dsp_tags(
            [2676008490401,5061089110801],
            '2021-04-01',
            '2021-06-30',
            'day',
            'AM,BP,CQ,DC,UZ,XM',
            []
        )
    elif method == 'sa_tags':
        results = data_service.sa_tags(
            [90766455496743,11166462748649],
            '2021-04-01',
            '2021-06-30',
            'day',
            'AM,BP,CQ,DC,UZ,XM',
        )
    elif method == 'tag_statistics':
        results = data_service.tag_statistics(
            [90766455496743,11166462748649],
            [2676008490401,5061089110801],
            '2021-04-01',
            '2021-06-30',
            'AM,BP,CQ,DC,UZ,XM',
            []
        )

    print(results)
