from amazon_api.resources.aa.audiences.api.audience import Audience

from amazon_api.resources.aa.v1.dsp.api.app import App
from amazon_api.resources.aa.v1.dsp.api.creative import Creative
from amazon_api.resources.aa.v1.dsp.api.domain_list import DomainList
from amazon_api.resources.aa.v1.dsp.api.geo_location import GeoLocation
from amazon_api.resources.aa.v1.dsp.api.goal_configuration import GoalConfiguration
from amazon_api.resources.aa.v1.dsp.api.iab_content_category import IabContentCategory
from amazon_api.resources.aa.v1.dsp.api.line_item import LineItem
from amazon_api.resources.aa.v1.dsp.api.order import Order
from amazon_api.resources.aa.v1.dsp.api.pixel import Pixel
from amazon_api.resources.aa.v1.dsp.api.pre_bid_targeting import PreBidTargeting
from amazon_api.resources.aa.v1.dsp.api.product_category import ProductCategory
from amazon_api.resources.aa.v1.dsp.api.supply_source import SupplySource

from amazon_api.resources.aa.v1.sb.api.ad_group import AdGroup as SBAdGroup
from amazon_api.resources.aa.v1.sd.api.ad_group import AdGroup as SDAdGroup
from amazon_api.resources.aa.v2.sp.api.ad_group import AdGroup as SPAdGroup

from amazon_api.resources.aa.v1.sb.api.budget_rule import BudgetRule as SBBudgetRule
from amazon_api.resources.aa.v2.sp.api.budget_rule import BudgetRule as SPBudgetRule

from amazon_api.resources.aa.v1.sb.api.campaign import Campaign as SBCampaign
from amazon_api.resources.aa.v1.sd.api.campaign import Campaign as SDCampaign
from amazon_api.resources.aa.v2.sp.api.campaign import Campaign as SPCampaign

from amazon_api.resources.aa.v1.sb.api.campaign_budget_rule import CampaignBudgetRule as SBCampaignBudgetRule
from amazon_api.resources.aa.v2.sp.api.campaign_budget_rule import CampaignBudgetRule as SPCampaignBudgetRule

from amazon_api.resources.aa.v1.sb.api.campaign_budget_rule_recommendation import CampaignBudgetRuleRecommendation as SBCampaignBudgetRuleRecommendation
from amazon_api.resources.aa.v2.sp.api.campaign_budget_rule_recommendation import CampaignBudgetRuleRecommendation as SPCampaignBudgetRuleRecommendation

from amazon_api.resources.aa.v1.sb.api.keyword import Keyword as SBKeyword
from amazon_api.resources.aa.v2.sp.api.keyword import Keyword as SPKeyword

from amazon_api.resources.aa.v1.sd.api.product_ad import ProductAd as SDProductAd
from amazon_api.resources.aa.v2.sp.api.product_ad import ProductAd as SPProductAd

from amazon_api.resources.aa.v2.portfolios.api.portfolio import Portfolio
from amazon_api.resources.aa.v2.profiles.api.profile import Profile

from amazon_api.resources.aa.v1.sb.api.target import Target as SBTarget
from amazon_api.resources.aa.v1.sd.api.target import Target as SDTarget
from amazon_api.resources.aa.v2.sp.api.target import Target as SPTarget

from server.core.constants import Constants


class AAUtility:
    """Associates a class with a specific Amazon Advertising API.

    AAUtility also provides the correct version of the API to use
    with the class.
    """

    def ad_group_interface_klass(self, api):
        if api == Constants.SPONSORED_BRANDS:
            return SBAdGroup
        elif api == Constants.SPONSORED_DISPLAY:
            return SDAdGroup
        elif api == Constants.SPONSORED_PRODUCTS:
            return SPAdGroup
    
    def app_interface_klass(self):
        return App
    
    def audience_interface_klass(self):
        return Audience
    
    def budget_rule_interface_klass(self, api):
        if api == Constants.SPONSORED_BRANDS:
            return SBBudgetRule
        elif api == Constants.SPONSORED_PRODUCTS:
            return SPBudgetRule
    
    def campaign_budget_rule_interface_klass(self, api):
        if api == Constants.SPONSORED_BRANDS:
            return SBCampaignBudgetRule
        elif api == Constants.SPONSORED_PRODUCTS:
            return SPCampaignBudgetRule
    
    def campaign_budget_rule_recommendation_interface_klass(self, api):
        if api == Constants.SPONSORED_BRANDS:
            return SBCampaignBudgetRuleRecommendation
        elif api == Constants.SPONSORED_PRODUCTS:
            return SPCampaignBudgetRuleRecommendation
    
    def campaign_interface_klass(self, api):
        if api == Constants.SPONSORED_BRANDS:
            return SBCampaign
        elif api == Constants.SPONSORED_DISPLAY:
            return SDCampaign
        elif api == Constants.SPONSORED_PRODUCTS:
            return SPCampaign
    
    def creative_interface_klass(self):
        return Creative
    
    def domain_list_interface_klass(self):
        return DomainList
    
    def geo_location_interface_klass(self):
        return GeoLocation
    
    def goal_configuration_interface_klass(self):
        return GoalConfiguration
    
    def iab_content_category_interface_klass(self):
        return IabContentCategory
    
    def keyword_interface_klass(self, api):
        if api == Constants.SPONSORED_BRANDS:
            return SBKeyword
        elif api == Constants.SPONSORED_PRODUCTS:
            return SPKeyword
    
    def line_item_interface_klass(self):
        return LineItem

    def line_item_creative_association_interface_klass(self):
        return LineItem

    def order_interface_klass(self):
        return Order
    
    def pixel_interface_klass(self):
        return Pixel

    def portfolio_interface_klass(self):
        return Portfolio
    
    def pre_bid_targeting_interface_klass(self):
        return PreBidTargeting
    
    def product_ad_interface_klass(self, api):
        if api == Constants.SPONSORED_DISPLAY:
            return SDProductAd
        elif api == Constants.SPONSORED_PRODUCTS:
            return SPProductAd
    
    def product_category_interface_klass(self):
        return ProductCategory
    
    def profile_interface_klass(self):
        return Profile
    
    def supply_source_interface_klass(self):
        return SupplySource
    
    def target_interface_klass(self, api):
        if api == Constants.SPONSORED_BRANDS:
            return SBTarget
        elif api == Constants.SPONSORED_DISPLAY:
            return SDTarget
        elif api == Constants.SPONSORED_PRODUCTS:
            return SPTarget
    
    def version_for_api(self, api):
        if api in [Constants.DSP, Constants.SPONSORED_BRANDS, Constants.SPONSORED_DISPLAY]:
            return Constants.V1

        return Constants.V2
