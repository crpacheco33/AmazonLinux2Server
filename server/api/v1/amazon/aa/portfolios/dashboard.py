from typing import (
    Any,
    Optional,
)

from fastapi import APIRouter
from fastapi import (
    APIRouter,
    Request,
    Response,
)
from fastapi.params import (
    Depends,
)

from server.core.constants import Constants
from server.dependencies import (
    advertising_data,
    brand,
    Interface,
    portfolios_client,
    read,
    sb_client,
    sp_client,
)
from server.resources.types.data_types import (
    IntervalType,
    ObjectiveType,
)
from server.services.aws_service import AWSService
from server.services.data_service import DataService
from server.utilities.aa_utility import AAUtility
from server.utilities.data_utility import DataUtility


aa_utility = AAUtility()
data_utility = DataUtility()
portfolio_klass = aa_utility.portfolio_interface_klass()
portfolios_interface = Interface(portfolios_client, portfolio_klass)
sb_klass = aa_utility.campaign_interface_klass(
    Constants.SPONSORED_BRANDS,
)
sb_interface = Interface(sb_client, sb_klass)
sp_klass = aa_utility.campaign_interface_klass(
    Constants.SPONSORED_PRODUCTS,
)
sp_interface = Interface(sp_client, sp_klass)
aws_service = AWSService()
log = aws_service.log_service
router = APIRouter(
    prefix=f'{Constants.PORTFOLIOS_PREFIX}{Constants.GRAPHS_PREFIX}',
)


@router.get(
    Constants.INDEX_PREFIX,
    dependencies=[
        Depends(read),
    ],
)
async def index(
    request: Request,
    response: Response,
    from_date: str,
    to_date: str,
    interval: IntervalType,
    objectives: Optional[str] = None,
    brand: Any = Depends(
        brand,
    ),
    source: DataService = Depends(
        advertising_data,
    ),
):
    log.info(
        f'Querying portfolio dashboard data...',
    )

    objectives, segments = data_utility.to_objectives_and_segments(objectives)

    portfolios = await portfolios_interface.index(
        advertiser_id=brand.amazon.aa.sa.advertiser_id,
        request=request,
        response=response,
    )
    
    campaign_ids = []
    for portfolio in portfolios:
        
        portfolio_id = portfolio.get('portfolioId')
        
        sb_campaigns = await sb_interface.index(
            advertiser_id=brand.amazon.aa.sa.advertiser_id,
            request=_request('sb', request.query_params, portfolio_id),
            response=response,
        )
        
        sp_campaigns = await sp_interface.index(
            advertiser_id=brand.amazon.aa.sa.advertiser_id,
            request=_request('sp', request.query_params, portfolio_id),
            response=response,
        )

        sb_campaign_ids = [int(campaign.get('campaignId')) for campaign in sb_campaigns]
        sp_campaign_ids = [int(campaign.get('campaignId')) for campaign in sp_campaigns]
    
        campaign_ids.extend(sb_campaign_ids)
        campaign_ids.extend(sp_campaign_ids)
        
    response = source.portfolios_dashboard(
        from_date,
        to_date,
        campaign_ids,
        interval.value,
        objectives,
    )

    log.info(
        f'Queried portfolio dashboard data',
    )

    return response


def _request(api, query_string, portfolioIdFilter):
    return Request(
        scope={
            'headers': [],
            'method': 'GET',
            'scheme': 'http',
            'server': ('getvisibly.com', 443,),
            'path': f'/api/v1/amazon/aa/{api}/campaigns',
            'query_string': f'{query_string}&portfolioIdFilter={portfolioIdFilter}'.encode(),
            'type': 'http',
        }
    )