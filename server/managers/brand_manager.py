"""Stores brand metadata for `server`."""


from server.decorators.singleton_decorator import singleton


@singleton
class BrandManager:
    """Provides a singleton resource for `brand`.

    BrandManager is equivalent to the `brand` dependency that is available
    to FastAPI requests, however, BrandManager is available outside of requests
    and enables metadata to be used when defining API clients, e.g., sd_client.
    """

    def __init__(self):
        self._brand = None

    @property
    def brand(self):
        """Brand object from DocumentDB."""
        return self._brand

    @brand.setter
    def brand(self, value):
        self._brand = value
