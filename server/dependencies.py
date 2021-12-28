from typing import (
    Any,
    Optional,
)

import logging
import os
import sys

from bson.objectid import ObjectId
from fastapi import (
    Request,
    Response,
)
from fastapi.params import Depends
from fastapi.security import SecurityScopes
from starlette.exceptions import HTTPException
from starlette.status import HTTP_403_FORBIDDEN

import pymongo

from amazon_api.resources.aa.client import Client

from server.core.constants import Constants
from server.delegates.aa_delegate import AADelegate
from server.managers.brand_manager import BrandManager
from server.resources.models.brand import Brand
from server.resources.models.user import User
from server.resources.schema.token import Token
from server.resources.types.data_types import ScopeType
from server.services.auth_service import AuthService
from server.services.aws_service import AWSService
from server.services.data_service import DataService
from server.services.insight_service import InsightService
from server.services.twilio_service import TwilioService
from server.utilities.aa_utility import AAUtility
from server.utilities.auth_utility import AuthUtility
from server.utilities.token_utility import Bearer


aws_service = AWSService()
bearer = Bearer()
log = aws_service.log_service
log.handler = logging.StreamHandler(
    sys.stdout,
)


def docdb():
    return aws_service.docdb_service.client


async def user(
    credentials: Token = Depends(
        bearer,
    ),
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
):
    return User.find_by_id(
        ObjectId(credentials.claims.id),
        client,
    )


async def brand(
    client: pymongo.MongoClient = Depends(
        docdb,
    ),
    credentials: Token = Depends(
        bearer,
    ),
):
    brand_id = credentials.claims.brand
    brand = Brand.find_by_id(
        ObjectId(brand_id),
        client,
    )

    brand_manager = BrandManager()
    brand_manager.brand = brand
    
    return brand


async def advertising_data():
    es_service = AWSService().es_service
    es_service.domain = AWSService().ssm_service.amazon_advertising_elasticsearch_domain

    return DataService(es_service.es_service)


async def admin(
    credentials: Token = Depends(
        bearer,
    ),
):
    user_scopes = credentials.claims.scopes
    if ScopeType.ADMIN not in user_scopes:
        raise HTTPException(
            HTTP_403_FORBIDDEN,
            detail=Constants.USER_FORBIDDEN,
        )

    return True


async def insight_service():
    return InsightService()

async def ssm_service():
    return aws_service.ssm_service


async def twilio_service():
    return TwilioService()


async def auth_service(
    ssm_service: AWSService.SSMService = Depends(
        ssm_service,
    ),
    twilio_service: TwilioService = Depends(
        twilio_service,
    )
):
    return AuthService(
        ssm_service,
        twilio_service,
    )


# Amazon

def dsp_client():
    aa_utility = AAUtility()
    brand_manager = BrandManager()
    
    ssm_service = aws_service.ssm_service
    shared_ssm_service = aws_service.shared_ssm_service
    
    region = brand_manager.brand.amazon.aa.region
    
    version = aa_utility.version_for_api(
        Constants.DSP,
    )
    aa_delegate = AADelegate(
        Constants.DSP,
        region,
        version,
        ssm_service,
        shared_ssm_service,
    )

    return Client(aa_delegate)


def portfolios_client():
    aa_utility = AAUtility()
    brand_manager = BrandManager()
    
    ssm_service = aws_service.ssm_service
    shared_ssm_service = aws_service.shared_ssm_service
    
    region = brand_manager.brand.amazon.aa.region
    
    version = aa_utility.version_for_api(
        Constants.PORTFOLIOS,
    )
    aa_delegate = AADelegate(
        Constants.PORTFOLIOS,
        region,
        version,
        ssm_service,
        shared_ssm_service,
    )

    return Client(aa_delegate)


def profiles_client():
    aa_utility = AAUtility()
    brand_manager = BrandManager()
    
    ssm_service = aws_service.ssm_service
    shared_ssm_service = aws_service.shared_ssm_service
    
    region = brand_manager.brand.amazon.aa.region
    
    version = aa_utility.version_for_api(
        Constants.PROFILES,
    )
    aa_delegate = AADelegate(
        Constants.PROFILES,
        region,
        version,
        ssm_service,
        shared_ssm_service,
    )

    return Client(aa_delegate)


async def read(
    credentials: Optional[Token] = Depends(
        bearer,
    ),
):
    user_scopes = credentials.claims.scopes
    if ScopeType.READ not in user_scopes:
        raise HTTPException(
            HTTP_403_FORBIDDEN,
            detail=Constants.USER_FORBIDDEN,
        )

    return True


def retail_data():
    es_service = AWSService().es_service
    es_service.domain = AWSService().ssm_service.amazon_retail_elasticsearch_domain

    return DataService(es_service.es_service)


def sb_client():
    aa_utility = AAUtility()
    brand_manager = BrandManager()
    
    ssm_service = aws_service.ssm_service
    shared_ssm_service = aws_service.shared_ssm_service
    
    region = brand_manager.brand.amazon.aa.region
    
    version = aa_utility.version_for_api(
        Constants.SPONSORED_BRANDS,
    )
    aa_delegate = AADelegate(
        Constants.SPONSORED_BRANDS,
        region,
        version,
        ssm_service,
        shared_ssm_service,
    )

    return Client(aa_delegate)


def sd_client():
    aa_utility = AAUtility()
    brand_manager = BrandManager()
    
    ssm_service = aws_service.ssm_service
    shared_ssm_service = aws_service.shared_ssm_service
    
    region = brand_manager.brand.amazon.aa.region
    
    version = aa_utility.version_for_api(
        Constants.SPONSORED_DISPLAY,
    )
    aa_delegate = AADelegate(
        Constants.SPONSORED_DISPLAY,
        region,
        version,
        ssm_service,
        shared_ssm_service,
    )

    return Client(aa_delegate)


def sp_client():
    aa_utility = AAUtility()
    brand_manager = BrandManager()
    
    ssm_service = aws_service.ssm_service
    shared_ssm_service = aws_service.shared_ssm_service
    
    region = brand_manager.brand.amazon.aa.region
    
    version = aa_utility.version_for_api(
        Constants.SPONSORED_PRODUCTS,
    )
    aa_delegate = AADelegate(
        Constants.SPONSORED_PRODUCTS,
        region,
        version,
        ssm_service,
        shared_ssm_service,
    )

    return Client(aa_delegate)


async def write(
    credentials: Token = Depends(
        bearer,
    ),
):
    user_scopes = credentials.claims.scopes
    if ScopeType.WRITE not in user_scopes:
        raise HTTPException(
            HTTP_403_FORBIDDEN,
            detail=Constants.USER_FORBIDDEN,
        )

    return True


class Interface:
    
    from server.decorators.cache_decorator import docdb_cache

    def __init__(self, client, klass):
        self._advertiser_id = None
        self._client = client
        self._klass = klass

        self._interface = None

    def __call__(self):
        return self

    @docdb_cache()
    async def index(self, advertiser_id: str, request: Request = None, response: Response = None):
        self._advertiser_id = advertiser_id
        return self.interface.index(**request.query_params)

    @docdb_cache()
    async def index_creative_association(self, advertiser_id: str, request: Request = None, response: Response = None):
        self._advertiser_id = advertiser_id
        return self.interface.index_creative_association(**request.query_params)

    async def create(self, advertiser_id: str, data, request: Request = None):
        self._advertiser_id = advertiser_id
        return self.interface.create(
            data,
            **request.query_params,
        ).json()

    async def index_create(self, advertiser_id: str, data: dict, request: Request = None, response: Response = None):
        self._advertiser_id = advertiser_id
        return self.interface.index_create(
            data=data,
            **request.query_params,
        ).json()

    async def register_brand(self, brand_name):
        return self.interface.register_brand(brand_name)

    @docdb_cache(is_many=False)
    async def show(self, advertiser_id: str, key, request: Request = None, response: Response = None):
        self._advertiser_id = advertiser_id
        return self.interface.show(
            key,
        ).json()

    async def update(self, advertiser_id: str, data, request: Request = None):
        self._advertiser_id = advertiser_id
        return self.interface.update(
            data,
            **request.query_params,
        ).json()

    async def destroy(self, advertiser_id: str, key, request: Request = None):
        self._advertiser_id = advertiser_id
        return self.interface.destroy(
            key,
            **request.query_params,
        ).json()

    @property
    def interface(self):
        #
        # Use `callable` to identify if client is a callable.
        #
        # Necessary to support test clients that are not callables.
        #
        if callable(self._client):
            self._interface = self._klass(
                self._client(),
                self._advertiser_id,
            )
        else:
            self._interface = self._klass(
                self._client,
                self._advertiser_id,
            )
        
        return self._interface
