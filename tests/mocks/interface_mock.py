import requests
import requests_mock

from tests.test_constants import TestConstants


class InterfaceMock():
    
    def __init__(self):
        mock_adapter = requests_mock.Adapter()
        mock_adapter.register_uri(
            requests_mock.ANY,
            requests_mock.ANY,
            json=[],
        )
        self._session = requests.Session()
        self._session.mount('https://', mock_adapter)

    def __call__(self):
        return self

    def index(self, url=None, **kwargs):
        return self_session.get(TestConstants.MOCK_URL)

    def index_create(self, url=None, data=None, **kwargs):
        return self_session.post(TestConstants.MOCK_URL)

    def index_extended(self, url=None, **kwargs):
        return self_session.get(TestConstants.MOCK_URL)

    def create(self, url=None, data=None, **kwargs):
        return self_session.post(TestConstants.MOCK_URL)

    def show(self, url=None):
        return self_session.get(TestConstants.MOCK_URL)

    def show_extended(self, url):
        return self_session.get(TestConstants.MOCK_URL)

    def patch(self, url=None, data=None, **kwargs):
        return self_session.patch(TestConstants.MOCK_URL)

    def update(self, url=None, data=None, **kwargs):
        return self_session.put(TestConstants.MOCK_URL)

    def destroy(self, url=None, **kwargs):
        return self_session.delete(TestConstants.MOCK_URL)