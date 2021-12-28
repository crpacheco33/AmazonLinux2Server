from starlette.config import Config
from starlette.datastructures import Secret


config = Config('server/.env')

ADVERTISER_NAME = config(
    'ADVERTISER_NAME',
    cast=str,
    default='test',
)
AMAZON_WEB_SERVICES_REGION = config(
    'AMAZON_WEB_SERVICES_REGION',
    cast=str,
    default='us-west-1',
)
AWS_IAM_ACCESS_KEY_ID = config(
    'AWS_IAM_ACCESS_KEY_ID',
    cast=str,
    default=None,
)
AWS_IAM_SECRET_ACCESS_KEY = config(
    'AWS_IAM_SECRET_ACCESS_KEY',
    cast=Secret,
    default=None,
)
AWS_IAM_SESSION_NAME = config(
    'AWS_IAM_SESSION_NAME',
    cast=str,
    default=None,
)
AWS_IAM_TRUSTING_ROLE = config(
    'AWS_IAM_TRUSTING_ROLE',
    cast=str,
    default=None,
)
ORIGIN = config(
    'ORIGIN',
    cast=str,
    default='getvisibly.com',
)