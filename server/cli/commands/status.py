import json
import logging
import sys
import traceback

import click
import requests

from server.core.constants import Constants
from server.services.aws_service import AWSService


log = AWSService().log_service
log.handler = logging.StreamHandler(
    sys.stdout,
)

class StatusContext:

    def __init__(self):
        pass


@click.group()
@click.pass_context
def status(ctx):
    ctx.obj = StatusContext()


@status.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.pass_obj
def ping(obj):
    try:
        response = requests.get(
            'http://127.0.0.1:8080/api/v1/status',
        )
        print(response.json())
        log.info(f'Ping status succeeded')
    except Exception as e:
        log.info(f'Ping status failed.')
        traceback.print_exc(e)
