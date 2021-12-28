import json
import logging
import os
import sys
import traceback

import click
import requests

from server.core.constants import Constants
from server.services.aws_service import AWSService

from server.resources.models.insight import Insight
from server.resources.models.report import (
    Report,
)


log = AWSService().log_service
log.handler = logging.StreamHandler(
    sys.stdout,
)

class ReportContext:

    def __init__(self):
        pass


@click.group()
@click.pass_context
def reports(ctx):
    ctx.obj = ReportContext()


@reports.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.pass_obj
def clean(obj):
    pass
    # session = DatabaseService().session
    # try:
    #     session.query(
    #         InsightTag,
    #     ).delete()
    #     session.query(
    #         ReportInsight,
    #     ).delete()
    #     session.query(
    #         Insight,
    #     ).delete()
    #     session.query(
    #         Tag,
    #     ).delete()
    #     session.query(
    #         Report,
    #     ).delete()
    #     session.commit()
    # except Exception as e:
    #     print(e)
    # finally:
    #     session.close()


@reports.command(
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
        log.info('Report data does not exist')
        exit(1)

    try:
        response = requests.post(
            'http://127.0.0.1:8080/api/v1/reports',
            json=data,
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(f'Report {data.get("title")} was created.')
    except Exception as e:
        log.info(f'Report {data.get("title")} was not created.')
        traceback.print_exc(e)


@reports.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.option('--path', '-p', required=True, type=click.Path(exists=True))
@click.pass_obj
def e2e(obj, path):
    access_token = os.getenv('ACCESS_TOKEN')
    data, error = _from_json(
        path,
    )
    
    if error is not None:
        log.info('E2E report data does not exist')
        exit(1)

    try:
        tag_data = data.get('tag')

        response = requests.post(
            'http://127.0.0.1:8080/api/v1/tags',
            json=tag_data,
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )

        insight_data = data.get('insight')
        insight_data['tags'] = [response.json().get('id')]
        
        response = requests.post(
            'http://127.0.0.1:8080/api/v1/insights',
            json=insight_data,
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )

        report_data = data.get('report')
        report_data['insights'] = [response.json().get('id')]
        
        response = requests.post(
            'http://127.0.0.1:8080/api/v1/reports',
            json=report_data,
            headers={
                'Authorization': f'Bearer {access_token}',
            },
        )
        log.info(f'E2E reports completed successfully.')
    except Exception as e:
        log.info(f'E2E reports failed.')
        traceback.print_exc(e)


def _from_json(file):
    result = None
    try:
        with open(file, 'rb') as readfile:
            result = json.load(readfile)
    except (IOError, TypeError) as e:
        return None, e

    return result, None
