from server.core.constants import Constants


class CacheUtility:
    """Utility methods used by `CacheDecorator`."""

    def stringify_id(self, item, path):
        """Transforms integer identifiers to strings for a single model (item).

        Amazon Sponsored Ads identifiers are big integers, but this size of
        integer is not supported in Javascript, which truncates the value. These
        identifiers are converted to `str` to maintain data integrity between
        `server` and `web`.

        Args:
            item: An Amazon Advertising model which may contain several identifiers
                to be stringified.
            path: The cache key that is used to reveal which model item represents

        Returns:
            The model item with its identifiers transformed into `str`s
        """
        keys_to_stringify = self._keys_to_stringify(path)

        for key in keys_to_stringify:
            item.update({
                key: str(item.get(key)),
            })

        return item

    def stringify_ids(self, items, path):
        """Transforms integer identifiers to strings for multiple models (items).

        Amazon Sponsored Ads identifiers are big integers, but this size of
        integer is not supported in Javascript, which truncates the value. These
        identifiers are converted to `str` to maintain data integrity between
        `server` and `web`.

        Args:
            items: List of Amazon Advertising models, each of which may contain
                several identifiers to be stringified.
            path: The cache key that is used to reveal which model item represents

        Returns:
            List of model items with their identifiers transformed into `str`s
        """
        keys_to_stringify = self._keys_to_stringify(path)

        for item in items:
            for key in keys_to_stringify:
                item.update({
                    key: str(item.get(key)),
                })

        return items

    def _keys_to_stringify(self, path):
        if 'ad_groups' in path:
            return ['adGroupId', 'campaignId']
        elif Constants.CAMPAIGNS in path:
            return ['campaignId', 'portfolioId']
        elif Constants.KEYWORDS in path:
            return ['adGroupId', 'campaignId', 'keywordId']
        elif Constants.PORTFOLIOS in path:
            return 'portfolioId'
        elif 'product_ads' in path:
            return ['adGroupId', 'adId', 'campaignId']
        elif Constants.TARGETS in path:
            return ['adGroupId', 'campaignId', 'targetId']
        else:
            return []
