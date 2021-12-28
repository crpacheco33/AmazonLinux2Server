from io import BytesIO

import datetime
import logging

from bson.objectid import ObjectId
from pymongo import ReturnDocument

import httpx
import pytest

from server.core.constants import Constants
from server.resources.models.report import Report
from server.resources.schema.insight import InsightCreateSchema
from server.resources.schema.report import (
    ReportCreateSchema,
)
from server.resources.types.data_types import (
    ReportStateType,
    ReportTemplateType,
)
from tests.test_constants import TestConstants


log = logging.getLogger(TestConstants.SERVER_TESTS)
log.setLevel(
    logging.INFO,
)


@pytest.mark.asyncio
@pytest.mark.reports
async def test_create_creates_report_and_publishes_event(mocker, read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = ReportCreateSchema(
        end_date=TestConstants.REPORT_END_DATE,
        insights=[],
        start_date=TestConstants.REPORT_START_DATE,
        template=ReportTemplateType.SA_OVERVIEW,
        title=TestConstants.REPORT_TITLE,
    )

    mocked_eb_service = mocker.patch('server.services.aws_service.AWSService.EBService.put_events')

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/reports',
            json=data.dict(),
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    report = response.json()
    
    expected = 200
    actual = response.status_code

    assert expected == actual

    expected = TestConstants.REPORT_TITLE
    actual = report.get('title')

    assert expected == actual

    expected = ReportStateType.PENDING
    actual = report.get('state')

    assert expected == actual

    mocked_eb_service.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.reports
async def test_create_creates_report_with_insights_and_tags(mocker, read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = {
        'action': TestConstants.INSIGHT_ACTION,
        'date': TestConstants.INSIGHT_DATE,
        'description': TestConstants.INSIGHT_DESCRIPTION,
        'title': TestConstants.INSIGHT_TITLE,
        'tags': [],
    }

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/insights',
            json=data,
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    insight_id = response.json().get('id')
        
    with test_client.start_session() as session:
        collection = test_client.visibly.tags
        result = collection.insert_one(
            {
                'name': TestConstants.TAG_NAME,
                'prefix': 'product',
                'brand_id': test_brand._id,
            }
        )
    
    data = ReportCreateSchema(
        end_date=TestConstants.REPORT_END_DATE,
        insights=[insight_id],
        start_date=TestConstants.REPORT_START_DATE,
        tags=[str(result.inserted_id)],
        template=ReportTemplateType.SA_OVERVIEW,
        title=TestConstants.REPORT_TITLE,
    )

    mocked_eb_service = mocker.patch('server.services.aws_service.AWSService.EBService.put_events')

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/reports',
            json=data.dict(),
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    report = response.json()
    
    expected = 200
    actual = response.status_code

    assert expected == actual

    insights = report.get('insights')
    tags = report.get('tags')

    expected = 1
    actual = len(insights)

    assert expected == actual

    expected = insight_id
    actual = insights[0]

    assert expected == actual

    expected = 1
    actual = len(tags)

    assert expected == actual

    expected = str(result.inserted_id)
    actual = tags[0]

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.reports
async def test_create_report_creates_report_only_if_user_has_write_access(mocker, read_only_user, read_only_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_only_user

    data = ReportCreateSchema(
        end_date=TestConstants.REPORT_END_DATE,
        insights=[],
        start_date=TestConstants.REPORT_START_DATE,
        template=ReportTemplateType.SA_OVERVIEW,
        title=TestConstants.REPORT_TITLE,
    )

    mocked_eb_service = mocker.patch('server.services.aws_service.AWSService.EBService.put_events')

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/reports',
            json=data.dict(),
            headers={
                'Authorization': f'Bearer {read_only_user_access_token}',
            },
        )

    expected = 403
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.reports
async def test_download_downloads_report(mocker, read_write_user, read_write_user_access_token, read_write_user_same_account, read_write_user_access_token_same_account, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = ReportCreateSchema(
        end_date=TestConstants.REPORT_END_DATE,
        insights=[],
        start_date=TestConstants.REPORT_START_DATE,
        template=ReportTemplateType.SA_OVERVIEW,
        title=TestConstants.REPORT_TITLE,
    )

    mocked_eb_service = mocker.patch('server.services.aws_service.AWSService.EBService.put_events')
    mocked_s3_service = mocker.patch('server.services.aws_service.AWSService.S3Service.download', return_value=BytesIO(b'Get Visibly'),)
    
    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/reports',
            json=data.dict(),
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    report_id = response.json().get('id')

    with test_client.start_session() as session:
        collection = test_client.visibly.reports
        report = collection.find_one_and_update(
            { '_id': ObjectId(report_id) },
            { '$set': { 'state': ReportStateType.READY } },
            return_document=ReturnDocument.AFTER,
        )

    expected = ReportStateType.READY
    actual = report.get('state')

    assert expected == actual

    # User
    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            f'/api/v1/reports/{report_id}/download',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    mocked_s3_service.assert_called_once()

    expected = 200
    actual = response.status_code

    assert expected == actual

    # Another user from same team
    app.dependency_overrides[user] = lambda: read_write_user_same_account

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            f'/api/v1/reports/{report_id}/download',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token_same_account}',
            },
        )

    expected = 2
    actual = mocked_s3_service.call_count
    
    assert expected == actual

    expected = 200
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.reports
async def test_download_does_not_download_reports_that_are_not_ready(mocker, read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = ReportCreateSchema(
        end_date=TestConstants.REPORT_END_DATE,
        insights=[],
        start_date=TestConstants.REPORT_START_DATE,
        template=ReportTemplateType.SA_OVERVIEW,
        title=TestConstants.REPORT_TITLE,
    )

    mocked_eb_service = mocker.patch('server.services.aws_service.AWSService.EBService.put_events')
    
    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/reports',
            json=data.dict(),
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    report_id = response.json().get('id')

    # Pending state

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            f'/api/v1/reports/{report_id}/download',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 404
    actual = response.status_code

    assert expected == actual

    # Error state
    with test_client.start_session() as session:
        collection = test_client.visibly.reports
        report = collection.find_one_and_update(
            { '_id': ObjectId(report_id) },
            { '$set': { 'state': ReportStateType.ERROR } },
            return_document=ReturnDocument.AFTER,
        )

    expected = ReportStateType.ERROR
    actual = report.get('state')

    assert expected == actual

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            f'/api/v1/reports/{report_id}/download',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 404
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.reports
async def test_destroy_report_deletes_report(mocker, read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = ReportCreateSchema(
        end_date=TestConstants.REPORT_END_DATE,
        insights=[],
        start_date=TestConstants.REPORT_START_DATE,
        template=ReportTemplateType.SA_OVERVIEW,
        title=TestConstants.REPORT_TITLE,
    )

    mocked_eb_service = mocker.patch('server.services.aws_service.AWSService.EBService.put_events')

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/reports',
            json=data.dict(),
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    report_id = response.json().get('id')

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.delete(
            f'/api/v1/reports/{report_id}',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.reports
async def test_destroy_report_does_not_delete_report_when_requested_from_another_user(mocker, read_write_user, read_write_user_access_token, read_write_user_same_account, read_write_user_access_token_same_account, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = ReportCreateSchema(
        end_date=TestConstants.REPORT_END_DATE,
        insights=[],
        start_date=TestConstants.REPORT_START_DATE,
        template=ReportTemplateType.SA_OVERVIEW,
        title=TestConstants.REPORT_TITLE,
    )

    mocked_eb_service = mocker.patch('server.services.aws_service.AWSService.EBService.put_events')

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/reports',
            json=data.dict(),
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    report_id = response.json().get('id')

    app.dependency_overrides[user] = lambda: read_write_user_same_account

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.delete(
            f'/api/v1/reports/{report_id}',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token_same_account}',
            },
        )

    expected = 403
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.reports
async def test_destroy_report_deletes_report_when_requested_by_admin_user(mocker, read_write_user, read_write_user_access_token, admin_user, admin_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = ReportCreateSchema(
        end_date=TestConstants.REPORT_END_DATE,
        insights=[],
        start_date=TestConstants.REPORT_START_DATE,
        template=ReportTemplateType.SA_OVERVIEW,
        title=TestConstants.REPORT_TITLE,
    )

    mocked_eb_service = mocker.patch('server.services.aws_service.AWSService.EBService.put_events')

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/reports',
            json=data.dict(),
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    report_id = response.json().get('id')

    app.dependency_overrides[user] = lambda: admin_user

    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.delete(
            f'/api/v1/reports/{report_id}',
            headers={
                'Authorization': f'Bearer {admin_user_access_token}',
            },
        )

    expected = 200
    actual = response.status_code

    assert expected == actual


@pytest.mark.asyncio
@pytest.mark.reports
async def test_insights_returns_insights_within_date_range(mocker, read_write_user, read_write_user_access_token, test_brand, test_client):
    from server.dependencies import (
        brand,
        docdb,
        user,
    )
    from server.main import app

    app.dependency_overrides[brand] = lambda: test_brand
    app.dependency_overrides[docdb] = lambda: test_client
    app.dependency_overrides[user] = lambda: read_write_user

    data = {
        'action': TestConstants.INSIGHT_ACTION,
        'date': TestConstants.INSIGHT_DATE,
        'description': TestConstants.INSIGHT_DESCRIPTION,
        'title': TestConstants.INSIGHT_TITLE,
        'tags': [],
    }


    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.post(
            '/api/v1/insights',
            json=data,
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    insight_id = response.json().get('id')
        
    async with httpx.AsyncClient(app=app, base_url='https://test.getvisibly.com') as test_app:
        response = await test_app.get(
            '/api/v1/reports/insights?from_date=3001-01-01&to_date=3001-01-31',
            headers={
                'Authorization': f'Bearer {read_write_user_access_token}',
            },
        )

    insights = response.json()
    
    expected = 200
    actual = response.status_code

    assert expected == actual

    expected = 1
    actual = len(insights)

    assert expected == actual

    expected = insight_id
    actual = insights[0].get('id')

    assert expected == actual