import signal
import sys

import click

from server.cli.commands import (
    amazon,
    brand,
    cache,
    data,
    database,
    insight,
    report,
    status,
    tag,
    user,
)


@click.group()
def visibly():
    pass


visibly.add_command(amazon.amazon)
visibly.add_command(brand.brands)
visibly.add_command(cache.cache)
visibly.add_command(data.data)
visibly.add_command(database.database)
visibly.add_command(insight.insights)
visibly.add_command(report.reports)
visibly.add_command(status.status)
visibly.add_command(tag.tags)
visibly.add_command(user.users)


def signal_handler(signal, frame):
    print(
        'Exiting Visibly due to user request',
    )
    sys.exit(0)


if __name__ == '__main__':
    try:
        signal.signal(
            signal.SIGINT,
            signal_handler,
        )
        visibly()
    except Exception as e:
        raise e
        sys.exit(1)
