import pytest

from server.utilities.cache_utility import CacheUtility

from tests.test_constants import TestConstants


@pytest.mark.utility
def test_stringify_id_stringifies_id_in_dict():
    cache_utility = CacheUtility()

    keygroup = [
        ['adGroupId', 'campaignId'],
        ['campaignId', 'portfolioId'],
        ['adGroupId', 'campaignId', 'keywordId'],
        ['adId', 'adGroupId', 'campaignId'],
        ['adGroupId', 'campaignId', 'targetId'],
    ]

    paths = ['ad_groups', 'campaigns', 'keywords', 'product_ads', 'targets']

    item = {
        'adId': 0,
        'adGroupId': 1,
        'campaignId': 2,
        'keywordId': 3,
        'portfolioId': 4,
        'targetId': 5,
    }

    for idx, path in enumerate(paths):
        keys = keygroup[idx]

        actual_item = item.copy()
        for key in keys:
            assert isinstance(actual_item[key], int)
        
        actual_item = cache_utility.stringify_id(actual_item, path)

        for key in keys:
            expected = str(item[key])
            actual = actual_item[key]

            assert expected == actual

            assert isinstance(actual_item[key], str)


@pytest.mark.utility
def test_stringify_ids_stringifies_ids_in_list():
    cache_utility = CacheUtility()

    keygroup = [
        ['adGroupId', 'campaignId'],
        ['campaignId', 'portfolioId'],
        ['adGroupId', 'campaignId', 'keywordId'],
        ['adId', 'adGroupId', 'campaignId'],
        ['adGroupId', 'campaignId', 'targetId'],
    ]

    paths = ['ad_groups', 'campaigns', 'keywords', 'product_ads', 'targets']

    items = [
        {
            'adId': 0,
            'adGroupId': 1,
            'campaignId': 2,
            'keywordId': 3,
            'portfolioId': 4,
            'targetId': 5,
        },
        {
            'adId': 6,
            'adGroupId': 7,
            'campaignId': 8,
            'keywordId': 9,
            'portfolioId': 10,
            'targetId': 11,
        },
    ]

    for idx, path in enumerate(paths):
        keys = keygroup[idx]

        for item in items:

            actual_item = item.copy()
            for key in keys:
                assert isinstance(actual_item[key], int)
            
            actual_item = cache_utility.stringify_id(actual_item, path)

            for key in keys:
                expected = str(item[key])
                actual = actual_item[key]

                assert expected == actual

                assert isinstance(actual_item[key], str)
