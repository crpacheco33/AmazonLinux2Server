from amazon_api.resources.aa.client import Client

from server.core.constants import Constants
from server.delegates.aa_delegate import AADelegate
from server.resources.types.data_types import InsightMetaType
from server.services.aws_service import AWSService
from server.utilities.aa_utility import AAUtility


class InsightService:
    """Manages user interaction with automated insights.
    
    Automated insights recommend to users to take an action, for example,
    to increase budget. These actions are accepted or dismissed by users.
    """

    def __init__(self):
        self._aa_utility = AAUtility()
        self._client = None

    def accept(self, insight):
        """Accepts and completes action recommended by insight.

        Args:
            insight: Document describing automated insight

        Returns:
            None

        Raises:
            HTTPError: An error occurred completing action
        """
        insight_type = insight.get(
            Constants.INSIGHT_TYPE,
        )

        if insight_type == InsightMetaType.BUDGET_RECOMMENDATION:
            ad_type = insight.get(
                'ad_type',
            )
            advertiser_id = insight.get(
                'advertiser_id',
            )
            region = insight.get('region')
            value = insight.get('value')
            campaign_id = insight.get('campaign_id')
            
            client = self._client(
                region,
                ad_type,
            )

            klass = self._aa_utility.campaign_interface_klass(ad_type)
            interface = klass(
                client,
                advertiser_id,
            )

            value = [
                {
                    'campaignId': campaign_id,
                    'budget': value,
                }
            ]

            if ad_type == Constants.SPONSORED_PRODUCTS:
                value = [
                    {
                        'campaignId': campaign_id,
                        'dailyBudget': value,
                    }
                ]

            response = interface.update(value)

            response.raise_for_status()

    # TODO(declan.ryan@getvisibly.com) Decide whether this method will
    # ever be implemented.
    def dismiss(self, insight):
        pass

    def _client(self, region, ad_type):
        aws_service = AWSService()
        ssm_service = aws_service.ssm_service
        shared_ssm_service = aws_service.shared_ssm_service
        
        version = self._aa_utility.version_for_api(
            ad_type,
        )
        aa_delegate = AADelegate(
            ad_type,
            region,
            version,
            ssm_service,
            shared_ssm_service,
        )

        return Client(aa_delegate)