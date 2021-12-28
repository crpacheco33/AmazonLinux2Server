import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from server.api.v1.base import router
from server.core.constants import Constants
from server.core import settings
from server.services.aws_service import AWSService


no_cors = os.getenv('NO_CORS')

# Origin go.getvisibly.com enables our pitch/demo environment
if Constants.GO_GETVISIBLY_COM in settings.ORIGIN:
    ALLOWED_ORIGINS = [
        f'https://go.getvisibly.com',
    ]
# Origin *.getvisibly.com enables our cloud environments
elif Constants.GETVISIBLY_COM in settings.ORIGIN:
    aws_service = AWSService()
    ALLOWED_ORIGINS = [
        f'https://{aws_service.advertiser_name}.getvisibly.com',
    ]
else:
# These origins enable local development
    ALLOWED_ORIGINS = [
        'http://getvisibly.local',
        'http://getvisibly.local:9090',
        'http://local.getvisibly.local',
    ]


def application():
    ssm_service = AWSService().ssm_service
    application = FastAPI(
        debug=True,
        title=Constants.APPLICATION_TITLE,
        version='1.0.0',
    )

    application.include_router(
        router,
        prefix=ssm_service.api_prefix,
    )

    if not no_cors:
        application = CORSMiddleware(
            application,
            allow_origins=ALLOWED_ORIGINS,
            allow_credentials=True,
            allow_methods=['*'],
            allow_headers=['*'],
        )
    
    return application


app = application()


if __name__ == '__main__':
    import uvicorn

    reload=True

    uvicorn.run(
        'server.main:app',
        host='0.0.0.0',
        port=8080,
        reload=reload,
        debug=True,
    )
