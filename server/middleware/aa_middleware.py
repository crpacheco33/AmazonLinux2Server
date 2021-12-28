"""Middleware that removes sensitive data from error messages.

Amazon's Advertising API can return messages that include the advertiser's
access token. This middleware removes the access token when present in an
error message.
"""

import json
import re
import requests

from fastapi.routing import APIRoute
from starlette.responses import (
    JSONResponse,
    Response,
)
from starlette.requests import Request
from starlette.status import (
    HTTP_500_INTERNAL_SERVER_ERROR,
)

from server.core.constants import Constants
from server.services.aws_service import AWSService


log = AWSService().log_service

class AAMiddleware(APIRoute):
    """Amazon Advertising middleware that intercepts error messages.
    
    The route handler handles errors by removing the access token from
    the response returned to the client.
    """

    def get_route_handler(self):
        _existing_handler = super().get_route_handler()

        async def handler(request: Request) -> Response:
            try:
                return await _existing_handler(request)
            except requests.exceptions.HTTPError as e:
                log.exception(e)

                filtered_response = self._filter(e.response)

                retry_after = e.response.headers.get(
                    Constants.RETRY_AFTER,
                    -1,
                )

                return JSONResponse(
                    status_code=e.response.status_code,
                    content={
                        Constants.MESSAGE: filtered_response,
                        Constants.RETRY_AFTER: retry_after,
                    }
                )
            except requests.exceptions.ConnectionError as e:
                # Timeouts, no response from server
                log.exception(e)
                return JSONResponse(
                    status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                    content={
                        Constants.MESSAGE: e.message,
                    }
                )
        
        return handler

    def _filter(response):
        filtered_response = re.sub(
            Constants.AA_TOKEN_PATTERN,
            Constants.AA_SUBSTITUTE_TOKEN_TEXT,
            response.text,
        )

        try:
            return json.loads(
                filtered_response,
            )
        except json.decoder.JSONDecodeError as e:
            log.exception(e)

    


