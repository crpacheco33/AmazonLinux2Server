import datetime
import json

from click.testing import CliRunner
from pydantic import ValidationError

import pymongo
import pytest

from server.cli.commands.user import users
from server.resources.models.brand import Brand
from server.resources.schema.onboard import UserOnboardSchema
from tests.test_constants import TestConstants


@pytest.mark.cli
def test_add_adds_user_to_database(test_client):
    _setup(test_client)
    runner = CliRunner()

    with runner.isolated_filesystem():
        with open('users.json', 'wt') as write_file:
            write_file.write(_unique_users())
        
        result = runner.invoke(
            users, ['add', '--path', 'users.json'],
        )
        assert result.exit_code == 0

        with test_client.start_session() as session:
            collection = test_client.visibly.users
            results = collection.find()

            expected = 2
            actual = len(list(results))

            assert expected == actual


@pytest.mark.cli
def test_add_adds_unique_users_only(test_client):
    _setup(test_client)
    runner = CliRunner()

    with runner.isolated_filesystem():
        with open('users.json', 'wt') as write_file:
            data = json.dumps([
                {
                    'email': f'{TestConstants.EMAIL} 1',
                    'name': f'{TestConstants.FULL_NAME} 1',
                    'brands': [
                        TestConstants.BRAND_NAME,
                        TestConstants.ALTERNATIVE_BRAND_NAME,
                    ],
                    'scopes': [
                        TestConstants.READ,
                        TestConstants.WRITE,
                    ],
                },
                {
                    'email': f'{TestConstants.EMAIL} 1',
                    'name': f'{TestConstants.FULL_NAME} 1',
                    'brands': [
                        TestConstants.BRAND_NAME,
                        TestConstants.ALTERNATIVE_BRAND_NAME,
                    ],
                    'scopes': [
                        TestConstants.READ,
                        TestConstants.WRITE,
                    ],
                }
            ])

            write_file.write(data)
        
        result = runner.invoke(
            users, ['add', '--path', 'users.json'],
        )
        assert result.exit_code == 0

        with test_client.start_session() as session:
            collection = test_client.visibly.users
            results = collection.find()

            expected = 1
            actual = len(list(results))

            assert expected == actual


@pytest.mark.cli
def test_onboard_transforms_csv_to_json(test_client):
    runner = CliRunner()

    result = runner.invoke(
        users, ['onboard', '--path', 'tests/fixtures/users.csv'],
    )
    assert result.exit_code == 0

    with open(f'users_{datetime.datetime.utcnow().date()}.json') as read_file:
        users_to_onboard = json.loads(read_file.read())

        for user_to_onboard in users_to_onboard:
            try:
                UserOnboardSchema(**user_to_onboard)
            except ValidationError as e:
                pytest.fail(str(e))


@pytest.mark.cli
def test_remove_removes_users(test_client):
    _setup(test_client)
    runner = CliRunner()

    ids = []

    with runner.isolated_filesystem():
        with open('users.json', 'wt') as write_file:
            write_file.write(_unique_users())
        
        result = runner.invoke(
            users, ['add', '--path', 'users.json'],
        )
        assert result.exit_code == 0

        with test_client.start_session() as session:
            collection = test_client.visibly.users
            results = collection.find()

            expected = 2
            actual = 0

            for result in results:
                actual += 1
                ids.append(str(result.get('_id')))
                
            assert expected == actual

    actual_ids = []

    with runner.isolated_filesystem():
        with open('users.json', 'wt') as write_file:
            data = json.dumps([
                { 'id': ids[0] },
            ])

            write_file.write(data)

        result = runner.invoke(
            users, ['remove', '--path', 'users.json'],
        )
        assert result.exit_code == 0

        with test_client.start_session() as session:
            collection = test_client.visibly.users
            results = collection.find()

            actual = 0

            for result in results:
                actual += 1
                actual_ids.append(str(result.get('_id')))

            expected = 1
            
            assert expected == actual

    expected = ids[-1]
    actual = actual_ids[0]

    assert expected == actual


@pytest.mark.cli
def test_remove_removes_users_from_brands(test_client):
    _setup(test_client)
    runner = CliRunner()

    ids = []

    with runner.isolated_filesystem():
        with open('users.json', 'wt') as write_file:
            write_file.write(_unique_users())
        
        result = runner.invoke(
            users, ['add', '--path', 'users.json'],
        )
        assert result.exit_code == 0

        with test_client.start_session() as session:
            collection = test_client.visibly.brands
            brands = collection.find()

            for brand in brands:
                expected = 0
                actual = len(brand.get('users'))

                assert expected == actual

        result = runner.invoke(
            users, ['invite', '--path', 'users.json'],
        )
        assert result.exit_code == 0

        with test_client.start_session() as session:
            collection = test_client.visibly.users
            results = collection.find()

            for result in results:
                ids.append(str(result.get('_id')))
                
            collection = test_client.visibly.brands
            brands = collection.find()

            for brand in brands:
                expected = 2
                actual = len(brand.get('users'))

                assert expected == actual

    with runner.isolated_filesystem():
        with open('users.json', 'wt') as write_file:
            data = json.dumps([
                { 'id': ids[0] },
            ])

            write_file.write(data)

        result = runner.invoke(
            users, ['remove', '--path', 'users.json'],
        )
        assert result.exit_code == 0

        with test_client.start_session() as session:
            collection = test_client.visibly.brands
            brands = collection.find()

            for brand in brands:
                expected = 1
                actual = len(brand.get('users'))

                assert expected == actual


def _setup(client):
    # Start without the context of application tests
    client.drop_database('visibly')

    client.visibly.brands.create_index(
        [( 'name', pymongo.DESCENDING,)],
        unique=True,
    )

    brands = [{
        'name': TestConstants.ALTERNATIVE_BRAND_NAME,
        'country': 'US',
        'timezone': 'America/Los_Angeles',
        'amazon': {
            'aa': {
                'dsp': {
                    'advertiser_id': '4093311673971939',
                    'entity_id': '1191625552155416',
                    'name': 'DSP',
                },
                'sa': {
                    'advertiser_id': '4093311673971939',
                    'name': 'SA',
                },
                'region': 'na',
            }
        },
    },
    {
        'name': TestConstants.BRAND_NAME,
        'country': 'US',
        'timezone': 'America/Los_Angeles',
        'amazon': {
            'aa': {
                'dsp': {
                    'advertiser_id': '2575637870401',
                    'entity_id': '1191625552155416',
                    'name': 'DSP',
                },
                'sa': {
                    'advertiser_id': '1110250468092771',
                    'name': 'SA',
                },
                'region': 'na',
            }
        },
    }]

    Brand.create_many(brands, client)


def _unique_users():
    return json.dumps([
        {
            'email': f'{TestConstants.EMAIL} 1',
            'name': f'{TestConstants.FULL_NAME} 1',
            'brands': [
                TestConstants.BRAND_NAME,
                TestConstants.ALTERNATIVE_BRAND_NAME,
            ],
            'scopes': [
                TestConstants.READ,
                TestConstants.WRITE,
            ],
        },
        {
            'email': f'{TestConstants.EMAIL} 2',
            'name': f'{TestConstants.FULL_NAME} 2',
            'brands': [
                TestConstants.BRAND_NAME,
                TestConstants.ALTERNATIVE_BRAND_NAME,
            ],
            'scopes': [
                TestConstants.READ,
                TestConstants.WRITE,
            ],
        }
    ])