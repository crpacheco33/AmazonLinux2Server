import click
import pymongo

from server.core.constants import Constants
from server.services.aws_service import AWSService


log = AWSService().log_service
client = AWSService().docdb_service.client


class DatabaseContext:

    def __init__(self):
        pass


@click.group()
@click.pass_context
def database(ctx):
    ctx.obj = DatabaseContext()


@database.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.argument('database')
@click.pass_obj
def drop(obj, database):
    databases = client.database_names()
    if database not in databases:
        log.info(
            f'{database} does not exist',
        )
        return

    log.info(
        f'Dropping database {database}...',
    )
    
    client.drop_database(database)

    log.info(
        f'Dropped database {database}',
    )


@database.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.argument('database')
@click.pass_obj
def index(obj, database):
    databases = client.database_names()
    if databases in databases:
        log.info(
            f'{database} database already exists',
        )
        return

    log.info(
        f'Creating indices on {database}...',
    )
    if database == 'visibly':
        with client.start_session(causal_consistency=True) as session:
            collection = client.visibly.brands
            collection.create_index(
                [('name', pymongo.DESCENDING)],
                unique=True,
            )

            collection = client.visibly.tags
            collection.create_index(
                [('name', pymongo.DESCENDING)],
                unique=True,
            )

            collection = client.visibly.users
            collection.create_index(
                [('email', pymongo.DESCENDING)],
                unique=True,
            )

    log.info(
        f'Created indices on {database}',
    )
    