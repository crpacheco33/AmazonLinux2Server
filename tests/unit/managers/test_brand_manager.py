import pytest

from server.managers.brand_manager import BrandManager
from tests.test_constants import TestConstants


@pytest.mark.asyncio
@pytest.mark.managers
def test_brand_manager_is_configured_by_brand_dependency(test_brand):
    brand_manager = BrandManager()

    expected = test_brand
    actual = brand_manager.brand

    assert expected == actual

    expected = TestConstants.BRAND_NAME
    actual = brand_manager.brand.name

    assert expected == actual

    expected = '1110250468092771'
    actual = brand_manager.brand.amazon.aa.sa.advertiser_id

    assert expected == actual