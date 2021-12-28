# TODO(declan.ryan@getvisibly.com) Remove from `server`.

from typing import Optional

import hashlib
import json

from fastapi import (
    Request,
    Response,
)

import redis

from server.core.constants import Constants
from server.services.aws_service import AWSService


class RedisService:

    class RedisCache:

        def __init__(self, ssm_service):
            self._client = None
            self._key_builder = None
            self._ssm_service = ssm_service

        def clear(self, key):
            self.client.delete(key)

        def get_json(self, key, is_many):
            if is_many:
                items = self.client.lrange(key, 0, -1)
                return [json.loads(item) for item in items]
            else:
                item = self.client.get(key)
                return json.loads(item)

        def get(self, key):
            return self.client.get(key)

        def set_json(self, key, value, is_many):
            if is_many:
                for item in value:
                    self.client.lpush(
                        key,
                        json.dumps(item),
                    )
            else:
                self.client.set(
                    key,
                    json.dumps(value),
                )
            
            self.client.expire(
                key,
                Constants.CACHE_TTL,
            )

        def set(self, key, value):
            self.client.set(
                key,
                value,
            )
            self.client.expire(
                key,
                Constants.CACHE_TTL,
            )

        def ttl(self, key):
            return self.client.ttl(key)

        @property
        def client(self):
            if self._client is None:
                self._client = redis.Redis(
                    host='localhost', #self._ssm_service.amazon_redis_host,
                    port=6379, #self._ssm_service.amazon_redis_port,
                    db=0,
                )

            return self._client

        @property
        def key_builder(self):
            if self._key_builder is None:
                def default_key_builder(
                    func,
                    advertiser_id: str,
                    request: Optional[Request] = None,
                    args: Optional[tuple] = None,
                    kwargs: Optional[dict] = None,
                ):
                    return hashlib.md5(
                        f'{advertiser_id}/{request.url.path}?{request.query_params}'.encode(Constants.UTF8),
                    ).hexdigest()
                
                self._key_builder = default_key_builder

            return self._key_builder
    
    __cache = None

    def __init__(self):
        pass

    @property
    def cache(self):
        if RedisService.__cache is None:
            ssm_service = AWSService().ssm_service
            RedisService.__cache = RedisService.RedisCache(ssm_service)

        return RedisService.__cache