from datetime import date
from typing import List

import json
import tempfile

from bson.objectid import ObjectId
from fastapi import APIRouter
from fastapi.params import (
    Depends,
)
from starlette.exceptions import HTTPException
from starlette.responses import FileResponse
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_403_FORBIDDEN,
    HTTP_404_NOT_FOUND,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

import botocore
import pymongo

from server.core.constants import Constants
from server.dependencies import (
    admin,
    brand,
    docdb,
    read,
    ssm_service,
    user,
    write,
)
from server.resources.models.brand import Brand
from server.resources.models.insight import Insight
from server.resources.models.report import Report
from server.resources.models.tag import Tag
from server.resources.models.user import User
from server.resources.schema.insight import InsightIndexSchema
from server.resources.schema.report import (
    ReportCreateSchema,
    ReportIndexSchema,
)
from server.resources.schema.tag import TagIndexSchema
from server.resources.types.data_types import (
    ReportStateType,
    ReportTemplateType,
    ScopeType,
)
from server.services.aws_service import AWSService


log = AWSService().log_service
router = APIRouter(
    prefix=Constants.REPORTS_PREFIX,
    tags=[Constants.REPORTS],
)


@router.get(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(read),
    ],
    response_model=List[ReportIndexSchema],
    response_model_by_alias=False,
)
def index(
    brand: Brand = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    ssm_service: AWSService.SSMService = Depends(
        ssm_service,
    )
):
    log.info(
        'Indexing reports...',
    )
    
    reports = Report.with_brand(
        ObjectId(brand._id),
        client,
    )

    log.info(
        f'Indexed reports',
    )
    
    return reports


@router.get(
    Constants.NEW_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
def new():
    return [template.value for template in ReportTemplateType]


@router.post(
    Constants.NO_PREFIX,
    dependencies=[
        Depends(write),
    ],
    response_model=ReportIndexSchema,
    response_model_by_alias=False,
)
def create(
    data: ReportCreateSchema,
    brand: Brand = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    ssm_service: AWSService.SSMService = Depends(
        ssm_service,
    ),
    user: User = Depends(
        user,
    )
):
    log.info(
        f'Creating report "{data.title}"...',
    )

    request_body = data.dict()
    request_body.update({
        'state': ReportStateType.PENDING,
        'brand_id': brand._id,
        'user_id': user._id,
    })

    insight_ids = request_body.pop(Constants.INSIGHTS, [])
    if insight_ids:
        insight_object_ids = [ObjectId(insight_id) for insight_id in insight_ids]
        request_body.update({ Constants.INSIGHTS: insight_object_ids })
    else:
        insight_object_ids = []

    tag_ids = request_body.pop(Constants.TAGS, [])
    if tag_ids:
        tag_object_ids = [ObjectId(tag_id) for tag_id in tag_ids]
        request_body.update({ Constants.TAGS: tag_object_ids })
    else:
        tag_object_ids = []
    
    try:
        report = Report.create(
            request_body,
            client,
        )
    except Exception as e:
        log.exception(e)
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=Constants.REPORT_CREATE_FAILURE,
        )

    try:
        Insight.add_report(
            insight_object_ids,
            report._id,
            brand._id,
            client,
        )
        
    except Exception as e:
        log.exception(e)

        Report.delete(
            report._id,
            client,
        )
        
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=INSIGHT_IDS_INVALID,
        )

    try:
        eb_service = _eb_service()
        eb_service.put_events(
            [
                dict(
                    Detail=json.dumps(
                        { Constants.ACTION: [Constants.CREATE] },
                    ),
                    EventBusName=ssm_service.event_bus,
                    DetailType=Constants.REPORT_EVENT_DETAIL_TYPE,
                    Source=Constants.REPORT_EVENT_SOURCE,
                )
            ]
        )
    except botocore.exceptions.ClientError as e:
        log.exception(e)
        raise HTTPException(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            detail=Constants.REPORT_EVENT_FAILURE,
        )

    log.info(
        f'Created report "{data.title}"',
    )

    return report


@router.get(
    '/{report_id}/download',
    dependencies=[
        Depends(read),
    ],
)
def download(
    report_id: str,
    brand: Brand = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    ssm_service: AWSService.SSMService = Depends(
        ssm_service,
    )
):
    log.info(
        f'Downloading report {report_id}...',
    )

    report = Report.find_by_id(
        ObjectId(report_id),
        client,
    )

    if report is None:
        
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail=Constants.REPORT_MISSING,
        )

    if report.state != ReportStateType.READY:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail=Constants.REPORT_NOT_READY,
        )

    key_components = [
        Constants.REPORTS,
        str(report.brand_id),
        report.template,
        f'{report_id}.{Constants.PDF}',
    ]
    key = Constants.FORWARD_SLASH.join(key_components)

    log.info(
        f'Downloading report from {key}...',
    )

    s3_service = _s3_service()
    downloaded_report = s3_service.download(
        key,
        ssm_service.bucket,
    )

    with tempfile.NamedTemporaryFile(
        mode='w+b',
        suffix=f'{report.template}.{Constants.PDF}',
        delete=False,
    ) as write_file:
        write_file.write(downloaded_report.read())
        return FileResponse(
            write_file.name,
            filename=f'{report.template}.{Constants.PDF}',
            media_type=Constants.APPLICATION_PDF,
        )

    log.info(
        f'Downloaded report {report_id}',
    )


@router.delete(
    '/{report_id}',
    dependencies=[
        Depends(write),
    ],
)
def destroy(
    report_id: str,
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    user: User = Depends(
        user,
    ),
):
    log.info(
        f'Destroying report {report_id}...'
    )

    report = Report.find_by_id(
        ObjectId(report_id),
        client,
    )
    
    if report.user_id == user._id:
        log.info(
            f'User {user._id} deleting report {report._id}...',
        )
    elif ScopeType.ADMIN in [scope for scope in user.scopes]:
        log.info(
            f'Admin user {user._id} deleting report {report._id}...',
        )

    else:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail=Constants.INSUFFICIENT_PERMISSIONS,
        )

    insight_ids = report.insights
    for insight_id in insight_ids:
        Insight.remove_report(
            insight_id,
            report_id,
            client,
        )    

    Report.delete(report._id, client)
    
    log.info(
        f'Destroyed report {report_id}',
    )


@router.get(
    Constants.INSIGHTS_PREFIX,
    dependencies=[
        Depends(read),
    ],
    response_model=List[InsightIndexSchema],
    response_model_by_alias=False,
)
def insights(
    from_date: str,
    to_date: str,
    brand: Brand = Depends(
        brand,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
) -> List[InsightIndexSchema]:
    log.info(
        f'Indexing report insights between {from_date} and {to_date}...',
    )

    insights = Insight.find_between_dates(
        from_date,
        to_date,
        brand._id,
        client,
    )

    log.info(
        f'Indexed report {len(insights)} insights between {from_date} and {to_date}',
    )

    return insights


def _eb_service():
    return AWSService().eb_service


def _s3_service():
    return AWSService().s3_service
