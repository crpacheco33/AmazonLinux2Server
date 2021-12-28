"""Transforms OpenSearch (Elasticsearch) responses into lists and dicts
for `web`.
"""


from collections import defaultdict
from datetime import(
    datetime,
    timedelta,
)


from server.core.constants import Constants
from server.managers.brand_manager import BrandManager
from server.resources.schema.amazon_api import IndexSearchTermsResponseSchema
from server.resources.types.data_types import (
    ApiType,
    FunnelType,
    ObjectiveType,
    TableType,
)
from server.services.aws_service import AWSService
from server.services.data.es import(
    # Advertising
    advertising_sales_and_total_sales_time_series,
    advertising_statistics,
    cumulative_sales_and_spend_time_series,
    dsp_and_sa_objectives_time_series,
    dsp_dashboard_time_series,
    dsp_model_aggregation,
    dsp_objectives_time_series,
    engagement_time_series,
    my_dashboard_statistics,
    portfolio_aggregation,
    portfolios_dashboard_time_series,
    sa_and_dsp_sales_vs_roas_time_series,
    sa_dashboard_time_series,
    sa_model_aggregation,
    sa_objectives_time_series,
    sales_and_spend_aggregation,
    sales_and_spend_by_objective_aggregation,
    total_attributed_sales_and_spend_aggregation,
    total_attributed_sales_and_spend_detail_aggregation,
    # Retail
    brand_analytics_statistics,
    brand_analytics_time_series,
    search_terms_filter,
    search_terms_periods,
    search_terms_time_series,
    search_terms,
    # Tags
    dsp_and_sa_tags_time_series,
    dsp_tags_time_series,
    sa_tags_time_series,
    tag_statistics,
)
from server.utilities.data_utility import DataUtility
from server.utilities.date_utility import DateUtility


log = AWSService().log_service


class DataService:

    def __init__(self, client):
        self._client = client

        self._brand_manager = BrandManager()
        self._data_utility = None
        self._date_utility = None

    # Advertising

    def advertising_sales_and_total_sales(self, api, start_date, end_date, interval):
        log.info(
            f'Querying advertising sales and total sales...',
        )

        response = {}

        try:
            time_series = advertising_sales_and_total_sales_time_series(
                self._client,
                self._index(api),
                start_date,
                end_date,
                interval,
            )
        except Exception as e:
            log.exception(e)
            return response

        buckets = time_series.aggregations.interval.buckets
        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )

            response[bucket_date] = {
                'dsp_advertising_sales': bucket.dsp_advertising_sales.value or 0,
                'dsp_total_sales': bucket.dsp_total_sales.value or 0,
                'sa_advertising_sales': bucket.sa_advertising_sales.value or 0,
                'sa_total_sales': bucket.sa_total_sales.value or 0,
            }

            try:
                response[bucket_date]['advertising_percent'] = bucket.advertising_percent.value or 0
            except AttributeError:
                response[bucket_date]['advertising_percent'] = 0

        log.info(
            f'Queried advertising sales and total sales',
        )

        return response

    def advertising_statistics(self, api, start_date, end_date, interval, table):
        log.info(
            f'Querying Advertising statistics {table}...',
        )

        response = {}

        try:
            time_series = advertising_statistics(
                self._client,
                self._index(api),
                start_date,
                end_date,
                interval,
                table,
            )
        except Exception as e:
            log.exception(e)
            return response

        buckets = time_series.aggregations.interval.buckets
        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )

            if table == TableType.SALES_AND_SPEND:
                response[bucket_date] = {
                    'dsp_sales': bucket.dsp_sales.value or 0,
                    'dsp_spend': bucket.dsp_spend.value or 0,
                    'dsp_total_sales': bucket.dsp_total_sales.value or 0,
                    'sa_sales': bucket.sa_sales.value or 0,
                    'sa_spend': bucket.sa_spend.value or 0,
                    'sa_total_sales': bucket.sa_total_sales.value or 0,
                }
            else:
                objective_buckets = bucket.objectives.buckets
                response[bucket_date] = {objective.value: {
                        'dsp_sales': 0,
                        'dsp_spend': 0,
                        'dsp_total_sales': 0,
                        'sa_sales': 0,
                        'sa_spend': 0,
                        'sa_total_sales': 0,
                    } for _, objective in ObjectiveType.__members__.items()}

                for objective_bucket in objective_buckets:
                    objective = objective_bucket.key
                    response[bucket_date][objective] = {
                        'dsp_sales': objective_bucket.dsp_sales.value or 0,
                        'dsp_spend': objective_bucket.dsp_spend.value or 0,
                        'dsp_total_sales': objective_bucket.dsp_total_sales.value or 0,
                        'sa_sales': objective_bucket.sa_sales.value or 0,
                        'sa_spend': objective_bucket.sa_spend.value or 0,
                        'sa_total_sales': objective_bucket.sa_total_sales.value or 0,
                    }
        
        log.info(
            f'Queried Advertising statistics {table}',
        )

        return response

    def cumulative_sales_and_spend(self, api, start_date, end_date, interval):
        log.info(
            f'Querying cumulative sales and spend...',
        )

        response = {}

        try:
            time_series = cumulative_sales_and_spend_time_series(
                self._client, 
                self._index(api),
                start_date, 
                end_date, 
                interval,
            )
        except Exception as e:
            log.exception(e)
            return response

        buckets = time_series.aggregations.interval.buckets
        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )

            response[bucket_date] = {
                'dsp_sales': bucket.dsp_sales.value or 0,
                'dsp_spend': bucket.dsp_spend.value or 0,
                'dsp_total_sales': bucket.dsp_total_sales.value or 0,
                'sa_sales': bucket.sa_sales.value or 0,
                'sa_spend': bucket.sa_spend.value or 0,
                'sa_total_sales': bucket.sa_total_sales.value or 0,
            }
            
            try:
                response[bucket_date]['roas'] = bucket.roas.value or 0
            except AttributeError:
                response[bucket_date]['roas'] = 0

            try:
                response[bucket_date]['total_roas'] = bucket.total_roas.value or 0
            except AttributeError:
                response[bucket_date]['total_roas'] = 0

        log.info(
            f'Queried cumulative sales and spend...',
        )

        return response     

    def dsp_and_sa_objectives(self, api, start_date, end_date, interval, objectives, segments):
        log.info(
            f'Querying DSP and SA objectives...',
        )

        response = {}

        try:
            time_series = dsp_and_sa_objectives_time_series(
                self._client,
                self._index(api),
                start_date,
                end_date,
                interval,
                objectives,
                segments,
            )
        except Exception as e:
            log.exception(e)
            return response

        buckets = time_series.aggregations.interval.buckets
        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )

            bucket_data = {
                'cpa': bucket.cpa.value or 0,
                'dsp_clicks': bucket.dsp_clicks.value or 0,
                'dsp_dpv': bucket.dsp_dpv.value or 0,
                'dsp_dpvr': bucket.dsp_dpvr.value or 0,
                'dsp_ecpm': bucket.dsp_ecpm.value or 0,
                'dsp_impressions': bucket.dsp_impressions.value or 0,
                'dsp_purchases_14d': bucket.dsp_purchases_14d.value or 0,
                'dsp_sales': bucket.dsp_sales.value or 0,
                'dsp_spend': bucket.dsp_spend.value or 0,
                'dsp_total_ntb_sales': bucket.dsp_total_ntb_sales.value or 0,
                'dsp_total_product_sales': bucket.dsp_total_product_sales.value or 0,
                'dsp_total_units_sold': bucket.dsp_total_units_sold.value or 0,
                'dsp_units_sold': bucket.dsp_units_sold.value or 0,
                'ntb_percentage': bucket.ntb_percentage.value or 0,
                'sa_attributed_conversions_14d': bucket.sa_attributed_conversions_14d.value or 0,
                'sa_clicks': bucket.sa_clicks.value or 0,
                'sa_dpv': bucket.sa_dpv.value or 0,
                'sa_impressions': bucket.sa_impressions.value or 0,
                'sa_sales': bucket.sa_sales.value or 0,
                'sa_spend': bucket.sa_spend.value or 0,
                'sa_total_ntb_sales': bucket.sa_total_ntb_sales.value or 0,
                'sa_total_product_sales': bucket.sa_total_product_sales.value or 0,
                'sa_total_units_sold': bucket.sa_total_units_sold.value or 0,
                'sa_units_sold': bucket.sa_units_sold.value or 0,
                'total_impressions': bucket.total_impressions.value or 0,
                'total_ntb_percentage': bucket.total_ntb_percentage.value or 0,
            }

            try:
                bucket_data['acpc'] = bucket.acpc.value or 0
            except AttributeError:
                bucket_data['acpc'] = 0

            try:
                bucket_data['cpa'] = bucket.cpa.value or 0
            except AttributeError:
                bucket_data['cpa'] = 0

            try:
                bucket_data['ctr'] = bucket.ctr.value or 0
            except AttributeError:
                bucket_data['ctr'] = 0

            try:
                bucket_data['dpvr'] = bucket.dpvr.value or 0
            except AttributeError:
                bucket_data['dpvr'] = 0

            try:
                bucket_data['ecpm'] = bucket.ecpm.value or 0
            except AttributeError:
                bucket_data['ecpm'] = 0

            try:
                bucket_data['roas'] = bucket.roas.value or 0
            except AttributeError:
                bucket_data['roas'] = 0

            try:
                bucket_data['total_roas'] = bucket.total_roas.value or 0
            except AttributeError:
                bucket_data['total_roas'] = 0

            response[bucket_date] = bucket_data

        log.info(
            f'Queried DSP and SA objectives',
        )

        return response

    def dsp_dashboard(self, order_ids, start_date, end_date, interval, segments, objectives):
        log.info(
            f'Querying DSP dashboard...',
        )
        
        response = {}

        try:
            time_series = dsp_dashboard_time_series(
                self._client,
                self._dsp_index(),
                order_ids,
                start_date,
                end_date,
                segments,
                objectives,
                interval,
            )
        except Exception as e:
            log.exception(e)
            return response

        buckets = time_series.aggregations.interval.buckets
        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )

            bucket_data = {
                'clicks': bucket.clicks.value or 0,
                'conversions': bucket.conversions.value or 0,
                'dpv': bucket.dpv.value or 0,
                'impressions': bucket.impressions.value or 0,
                'sales': bucket.sales.value or 0,
                'spend': bucket.spend.value or 0,
                'total_sales': bucket.total_sales.value or 0,
                'total_units_sold': bucket.total_units_sold.value or 0,
                'units_sold': bucket.units_sold.value or 0,
            }

            try:
                bucket_data['acos'] = bucket.acos.value or 0
            except AttributeError:
                bucket_data['acos'] = 0

            try:
                bucket_data['acpc'] = bucket.cpa.value or 0
            except AttributeError:
                bucket_data['acpc'] = 0

            try:
                bucket_data['cpa'] = bucket.cpa.value or 0
            except AttributeError:
                bucket_data['cpa'] = 0

            try:
                bucket_data['ctr'] = bucket.ctr.value or 0
            except AttributeError:
                bucket_data['ctr'] = 0

            try:
                bucket_data['cvr'] = bucket.cvr.value or 0
            except AttributeError:
                bucket_data['cvr'] = 0

            try:
                bucket_data['dpvr'] = bucket.dpvr.value or 0
            except AttributeError:
                bucket_data['dpvr'] = 0

            try:
                bucket_data['ecpm'] = bucket.ecpm.value or 0
            except AttributeError:
                bucket_data['ecpm'] = 0

            try:
                bucket_data['roas'] = bucket.roas.value or 0
            except AttributeError:
                bucket_data['roas'] = 0

            try:
                bucket_data['total_acos'] = bucket.total_acos.value or 0
            except AttributeError:
                bucket_data['total_acos'] = 0

            try:
                bucket_data['total_roas'] = bucket.total_roas.value or 0
            except AttributeError:
                bucket_data['total_roas'] = 0

            response[bucket_date] = bucket_data
        
        try:
            response['average_spend'] = {
                'value': time_series.aggregations.average_spend.value or 0,
            }
            response['average_sales'] = {
                'value': time_series.aggregations.average_sales.value or 0,
            }
            response['average_roas'] = {
                'value': time_series.aggregations.average_roas.value or 0,
            }
            response['average_total_roas'] = {
                'value': time_series.aggregations.average_total_roas.value or 0,
            }
            response['average_total_sales'] = {
                'value': time_series.aggregations.average_total_sales.value or 0,
            }
        except AttributeError as e:
            log.exception(e)
            response['average_spend'] = {
                'value': 0,
            }
            response['average_sales'] = {
                'value': 0,
            }
            response['average_roas'] = {
                'value': 0,
            }
            response['average_total_roas'] = {
                'value': 0,
            }
            response['average_total_sales'] = {
                'value': 0,
            }

        log.info(
            f'Queried DSP dashboard',
        )

        return response
    
    def dsp_model(self, api, model, model_ids, from_date, to_date):
        log.info(
            f'Querying DSP {model}s...',
        )

        if from_date and to_date:
            from_date, to_date = self.date_utility.to_es_dates(
                from_date,
                to_date,
            )

            from_date = self.date_utility.to_string(from_date)
            to_date = self.date_utility.to_string(to_date)

        response = defaultdict(dict)

        try:
            aggregate = dsp_model_aggregation(
                self._client,
                self._dsp_index(),
                model,
                model_ids,
                from_date,
                to_date,
            )
        except Exception as e:
            log.exception(e)
            return response

        for model_id in model_ids:
            response[model_id] = {
                'totalAttributedSales': 0,
                'totalClicks': 0,
                'totalImpressions': 0,
                'totalSales': 0,
                'totalSpend': 0,
                'totalUnitsSold': 0,
                'unitsSold': 0,
                'ctr': 0,
            }

        buckets = aggregate.aggregations.results.buckets
        for bucket in buckets:
            model_id = bucket.key
            response[model_id] = {
                'totalAttributedSales': bucket.total_attributed_sales.value,
                'totalClicks': bucket.total_clicks.value,
                'totalImpressions': bucket.total_impressions.value,
                'totalSales': bucket.total_sales.value,
                'totalSpend': bucket.total_spend.value,
                'totalUnitsSold': bucket.total_units_sold.value,
                'unitsSold': bucket.units_sold.value,
            }

            try:
                response[model_id]['ctr'] = bucket.ctr.value or 0
            except AttributeError:
                response[model_id]['ctr'] = 0
        
        log.info(
            f'Queried DSP {model}s',
        )
        
        return response

    def dsp_objectives(self, start_date, end_date, interval, objectives, segments):
        log.info(
            f'Querying DSP objectives...',
        )

        response = {}

        try:
            time_series = dsp_objectives_time_series(
                self._client,
                self._indices(),
                start_date,
                end_date,
                interval,
                objectives,
                segments,
            )
        except Exception as e:
            log.exception(e)
            return response

        buckets = time_series.aggregations.interval.buckets
        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )

            bucket_data = {
                'clicks': bucket.clicks.value or 0,
                'dpv': bucket.dpv.value or 0,
                'impressions': bucket.impressions.value or 0,
                'ntb_percentage': bucket.ntb_percentage.value or 0,
                'purchases': bucket.purchases.value or 0,
                'sales': bucket.sales.value or 0,
                'spend': bucket.spend.value or 0,
                'total_ntb_percentage': bucket.total_ntb_percentage.value or 0,
                'total_ntb_sales': bucket.total_ntb_sales.value or 0,
                'total_product_sales': bucket.total_product_sales.value or 0,
                'total_units_sold': bucket.total_units_sold.value or 0,
                'units_sold': bucket.units_sold.value or 0,
            }

            try:
                bucket_data['acpc'] = bucket.cpa.value or 0
            except AttributeError:
                bucket_data['acpc'] = 0

            try:
                bucket_data['cpa'] = bucket.cpa.value or 0
            except AttributeError:
                bucket_data['cpa'] = 0

            try:
                bucket_data['ctr'] = bucket.ctr.value or 0
            except AttributeError:
                bucket_data['ctr'] = 0

            try:
                bucket_data['dpvr'] = bucket.dpvr.value or 0
            except AttributeError:
                bucket_data['dpvr'] = 0

            try:
                bucket_data['ecpm'] = bucket.ecpm.value or 0
            except AttributeError:
                bucket_data['ecpm'] = 0

            try:
                bucket_data['roas'] = bucket.roas.value or 0
            except AttributeError:
                bucket_data['roas'] = 0

            try:
                bucket_data['total_roas'] = bucket.total_roas.value or 0
            except AttributeError:
                bucket_data['total_roas'] = 0

            response[bucket_date] = bucket_data

        log.info(
            f'Queried DSP objectives',
        )

        return response

    def engagement(self, api, start_date, end_date, interval):
        log.info(
            f'Querying engagement...',
        )

        response = {}

        try:
            engagement = engagement_time_series(
                self._client,
                self._index(api),
                start_date,
                end_date,
                interval,
            )
        except Exception as e:
            log.exception(e)
            return response

        buckets = engagement.aggregations.interval.buckets
        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )

            response[bucket_date] = {}

            response[bucket_date] = {
                'dsp_clicks': bucket.dsp_clicks.value or 0,
                'dsp_impressions': bucket.dsp_impressions.value or 0,
                'sa_clicks': bucket.sa_clicks.value or 0,
                'sa_impressions': bucket.sa_impressions.value or 0,
            }
            
            try:
                response[bucket_date]['ctr'] = bucket.ctr.value or 0
            except AttributeError:
                response[bucket_date]['ctr'] = 0
        
        log.info(
            f'Queried engagement',
        )

        return response

    def my_dashboard_statistics(self, api, start_date, end_date, interval, table):
        log.info(
            f'Querying My Dashboard statistics {table}...',
        )

        response = {}

        try:
            time_series = my_dashboard_statistics(
                self._client,
                self._index(api),
                start_date,
                end_date,
                interval,
                table,
            )
        except Exception as e:
            log.exception(e)
            return response

        buckets = time_series.aggregations.interval.buckets
        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )

            if table == TableType.ALL:
                response[bucket_date] = {
                    'dsp_clicks': bucket.dsp_clicks.value or 0,
                    'dsp_impressions': bucket.dsp_impressions.value or 0,
                    'dsp_sales': bucket.dsp_sales.value or 0,
                    'dsp_spend': bucket.dsp_spend.value or 0,
                    'dsp_total_sales': bucket.dsp_total_sales.value or 0,
                    'sa_clicks': bucket.sa_clicks.value or 0,
                    'sa_impressions': bucket.sa_impressions.value or 0,
                    'sa_sales': bucket.sa_sales.value or 0,
                    'sa_spend': bucket.sa_spend.value or 0,
                    'sa_total_sales': bucket.sa_total_sales.value or 0,
                }

                try:
                    response[bucket_date]['acpc'] = bucket.acpc.value or 0
                except AttributeError:
                    response[bucket_date]['acpc'] = 0
                
                try:
                    response[bucket_date]['ctr'] = bucket.ctr.value or 0
                except AttributeError:
                    response[bucket_date]['ctr'] = 0

                try:
                    response[bucket_date]['roas'] = bucket.roas.value or 0
                except AttributeError:
                    response[bucket_date]['roas'] = 0

                try:
                    response[bucket_date]['total_roas'] = bucket.total_roas.value or 0
                except AttributeError:
                    response[bucket_date]['total_roas'] = 0

            else:
                ad_type_buckets = bucket.ad_type.buckets

                fields = ['sales', 'spend', 'total_sales']
                ad_type_data = {
                    Constants.DSP: {
                        field: 0 for field in fields
                    },
                    Constants.SPONSORED_BRANDS: {
                        field: 0 for field in fields
                    },
                    Constants.SPONSORED_DISPLAY: {
                        field: 0 for field in fields
                    },
                    Constants.SPONSORED_PRODUCTS: {
                        field: 0 for field in fields
                    },
                }
                for ad_type_bucket in ad_type_buckets:
                    ad_type = ad_type_bucket.key
                    
                    ad_type_data[ad_type] = {
                        'sales': ad_type_bucket.sales.value or 0,
                        'spend': ad_type_bucket.spend.value or 0,
                    }

                    try:
                        ad_type_data[ad_type]['total_sales'] = ad_type_bucket.total_sales.value or 0
                    except AttributeError:
                        ad_type_data[ad_type]['total_sales'] = 0
                
                response[bucket_date] = ad_type_data

        log.info(
            f'Queried My Dashboard statistics {table}',
        )

        return response
        
    def portfolios_dashboard(self, start_date, end_date, campaign_ids, interval, objectives):
        log.info(
            f'Querying portfolios dashboard...',
        )

        response = {}

        try:
            time_series = portfolios_dashboard_time_series(
                self._client,
                self._sa_index(),
                start_date,
                end_date,
                campaign_ids,
                interval,
                objectives,
            )
        except Exception as e:
            log.exception(e)
            return response
        
        buckets = time_series.aggregations.interval.buckets
        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )

            response[bucket_date] = {
                'clicks': bucket.clicks.value or 0,
                'impressions': bucket.impressions.value or 0,
                'sales': bucket.sales.value or 0,
                'spend': bucket.spend.value or 0,
                'total_sales': bucket.total_sales.value or 0,
                'total_units_sold': bucket.total_units_sold.value or 0,
                'units_sold': bucket.units_sold.value or 0,
            }

            try:
                response[bucket_date]['acos'] = bucket.acos.value or 0
            except AttributeError:
                response[bucket_date]['acos'] = 0

            try:
                response[bucket_date]['ctr'] = bucket.ctr.value or 0
            except AttributeError:
                response[bucket_date]['ctr'] = 0

            try:
                response[bucket_date]['roas'] = bucket.roas.value or 0
            except AttributeError:
                response[bucket_date]['roas'] = 0

            try:
                response[bucket_date]['total_acos'] = bucket.total_acos.value or 0
            except AttributeError:
                response[bucket_date]['total_acos'] = 0

            try:
                response[bucket_date]['total_roas'] = bucket.total_roas.value or 0
            except AttributeError:
                response[bucket_date]['total_roas'] = 0

        response['average_spend'] = {
            'value': time_series.aggregations.average_spend.value or 0,
        }
        response['average_sales'] = {
            'value': time_series.aggregations.average_sales.value or 0,
        }
        response['average_roas'] = {
            'value': time_series.aggregations.average_roas.value or 0,
        }
        response['average_total_roas'] = {
            'value': time_series.aggregations.average_total_roas.value or 0,
        }
        response['average_total_sales'] = {
            'value': time_series.aggregations.average_total_sales.value or 0,
        }

        log.info(
            f'Queried portfolios dashboard',
        )

        return response
    
    def sa_and_dsp_sales_vs_roas(self, start_date, end_date, interval):
        log.info(
            f'Querying Sponsored Ads and DSP versus RoAS...',
        )

        response = {}

        try:
            time_series = sa_and_dsp_sales_vs_roas_time_series(
                self._client,
                self._indices(),
            )
        except Exception as e:
            log.exception(e)
            return response

        buckets = time_series.aggregations.interval.buckets

        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )

            response[bucket_date] = {
                'dsp_sales': bucket.dsp_sales.value or 0,
                'dsp_spend': bucket.dsp_spend.value or 0,
                'dsp_total_sales': bucket.dsp_total_sales.value or 0,
                'roas': bucket.roas.value or 0,
                'sa_sales': bucket.sa_sales.value or 0,
                'sa_spend': bucket.sa_spend.value or 0,
                'sa_total_sales': bucket.sa_total_sales.value or 0,
                'total_roas': bucket.total_roas.value or 0,
            }

        log.info(
            f'Queried Sponsored Ads and DSP versus RoAS',
        )

        return response

    def sa_dashboard(self, campaign_ids, start_date, end_date, objectives, ad_type, interval):
        log.info(
            f'Querying SA {ad_type} dashboard...',
        )

        response = {}

        try:
            time_series = sa_dashboard_time_series(
                self._client,
                self._sa_index(),
                campaign_ids,
                start_date,
                end_date,
                objectives,
                ad_type,
                interval,
            )
        except Exception as e:
            log.exception(e)
            return response

        buckets = time_series.aggregations.interval.buckets
        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )

            response[bucket_date] = {
                'clicks': bucket.clicks.value or 0,
                'impressions': bucket.impressions.value or 0,
                'sales': bucket.sales.value or 0,
                'spend': bucket.spend.value or 0,
                'total_sales': bucket.total_sales.value or 0,
                'units_sold': bucket.units_sold.value or 0,
            }

            if ad_type in [Constants.SPONSORED_DISPLAY, Constants.SPONSORED_PRODUCTS]:
                response[bucket_date]['total_units_sold'] = bucket.total_units_sold.value or 0

            try:
                response[bucket_date]['acos'] = bucket.acos.value or 0
            except AttributeError:
                response[bucket_date]['acos'] = 0

            try:
                response[bucket_date]['ctr'] = bucket.ctr.value or 0
            except AttributeError:
                response[bucket_date]['ctr'] = 0

            try:
                response[bucket_date]['roas'] = bucket.roas.value or 0
            except AttributeError:
                response[bucket_date]['roas'] = 0

            try:
                response[bucket_date]['total_acos'] = bucket.total_acos.value or 0
            except AttributeError:
                response[bucket_date]['total_acos'] = 0

            try:
                response[bucket_date]['total_roas'] = bucket.total_roas.value or 0
            except AttributeError:
                response[bucket_date]['total_roas'] = 0

        response['average_spend'] = {
            'value': time_series.aggregations.average_spend.value or 0,
        }
        response['average_sales'] = {
            'value': time_series.aggregations.average_sales.value or 0,
        }
        response['average_roas'] = {
            'value': time_series.aggregations.average_roas.value or 0,
        }
        response['average_total_roas'] = {
            'value': time_series.aggregations.average_total_roas.value or 0,
        }
        response['average_total_sales'] = {
            'value': time_series.aggregations.average_total_sales.value or 0,
        }

        log.info(
            f'Queried SA {ad_type} dashboard',
        )

        return response
    
    def sa_model(self, api, model, model_ids, from_date, to_date):
        log.info(
            f'Querying Sponsored Ads {model}s...',
        )

        if from_date and to_date:
            from_date, to_date = self.date_utility.to_es_dates(
                from_date,
                to_date,
            )

            from_date = self.date_utility.to_string(from_date)
            to_date = self.date_utility.to_string(to_date)

        response = defaultdict(dict)

        if model == Constants.PORTFOLIO:
            try:
                aggregate = portfolio_aggregation(
                    self._client,
                    self._sa_index(),
                    model_ids,
                    from_date,
                    to_date,
                )
            except Exception as e:
                log.exception(e)
                return response    

            response['campaigns'] = {
                'total_clicks': 0,
                'total_impressions': 0,
                'total_sales': 0,
                'total_spend': 0,
                'total_units_sold': 0,
                'units_sold': 0,
                'ctr': 0,
            }

            buckets = aggregate.aggregations.results.buckets
            
            for bucket in buckets:
                key = bucket.key
                response[key] = {
                    'total_clicks': bucket.total_clicks.value or 0,
                    'total_impressions': bucket.total_impressions.value or 0,
                    'total_sales': bucket.total_sales.value or 0,
                    'total_spend': bucket.total_spend.value or 0,
                    'total_units_sold': bucket.total_units_sold.value or 0,
                    'units_sold': bucket.units_sold.value or 0,
                }
                try:
                    response[key]['ctr'] = bucket.ctr.value or 0
                except AttributeError:
                    response[key]['ctr'] = 0
        else:
            for model_id in model_ids:
                response[str(model_id)] = {
                    'totalAttributedSales': 0,
                    'totalClicks': 0,
                    'totalImpressions': 0,
                    'totalSales': 0,
                    'totalSpend': 0,
                    'totalUnitsSold': 0,
                    'unitsSold': 0,
                    'total_attributed_sales': 0,
                    'total_clicks': 0,
                    'total_impressions': 0,
                    'total_sales': 0,
                    'total_spend': 0,
                    'total_units_sold': 0,
                    'units_sold': 0,
                    'ctr': 0,
                }
            
            try:
                aggregate = sa_model_aggregation(
                    self._client,
                    self._sa_index(),
                    api,
                    model,
                    model_ids,
                    from_date,
                    to_date,
                )
            except Exception as e:
                log.exception(e)
                return response

            buckets = aggregate.aggregations.results.buckets
            
            for bucket in buckets:
                key = bucket.key
                response[key] = {
                    'totalAttributedSales': bucket.total_attributed_sales.value or 0,
                    'total_attributed_sales': bucket.total_attributed_sales.value or 0,
                    'totalClicks': bucket.total_clicks.value or 0,
                    'total_clicks': bucket.total_clicks.value or 0,
                    'totalImpressions': bucket.total_impressions.value or 0,
                    'total_impressions': bucket.total_impressions.value or 0,
                    'totalSales': bucket.total_sales.value or 0,
                    'total_sales': bucket.total_sales.value or 0,
                    'totalSpend': bucket.total_spend.value or 0,
                    'total_spend': bucket.total_spend.value or 0,
                    'totalUnitsSold': bucket.total_units_sold.value or 0,
                    'total_units_sold': bucket.total_units_sold.value or 0,
                    'unitsSold': bucket.units_sold.value or 0,
                    'units_sold': bucket.units_sold.value or 0,
                }
                try:
                    response[key]['ctr'] = bucket.ctr.value or 0
                except AttributeError:
                    response[key]['ctr'] = 0

        log.info(
            f'Queried Sponsored Ads {model}s',
        )

        return response

    def sa_objectives(self, start_date, end_date, interval, objectives):
        log.info(
            f'Querying SA objectives...',
        )

        response = {}

        try:
            time_series = sa_objectives_time_series(
                self._client,
                self._sa_index(),
                start_date,
                end_date,
                interval,
                objectives,
            )
        except Exception as e:
            log.exception(e)
            return response

        buckets = time_series.aggregations.interval.buckets
        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )

            bucket_data = {
                'clicks': bucket.clicks.value or 0,
                'conversions': bucket.conversions.value or 0,
                'dpv': bucket.dpv.value or 0,
                'impressions': bucket.impressions.value or 0,
                'sales': bucket.sales.value or 0,
                'spend': bucket.spend.value or 0,
                'total_ntb_sales': bucket.total_ntb_sales.value or 0,
                'total_product_sales': bucket.total_product_sales.value or 0,
                'total_units_sold': bucket.total_units_sold.value or 0,
                'units_sold': bucket.units_sold.value or 0,
            }

            try:
                bucket_data['acpc'] = bucket.cpa.value or 0
            except AttributeError:
                bucket_data['acpc'] = 0

            try:
                bucket_data['cpa'] = bucket.cpa.value or 0
            except AttributeError:
                bucket_data['cpa'] = 0

            try:
                bucket_data['ctr'] = bucket.ctr.value or 0
            except AttributeError:
                bucket_data['ctr'] = 0

            try:
                bucket_data['dpvr'] = bucket.dpvr.value or 0
            except AttributeError:
                bucket_data['dpvr'] = 0

            try:
                bucket_data['ecpm'] = bucket.ecpm.value or 0
            except AttributeError:
                bucket_data['ecpm'] = 0

            try:
                bucket_data['roas'] = bucket.roas.value or 0
            except AttributeError:
                bucket_data['roas'] = 0

            try:
                bucket_data['total_roas'] = bucket.total_roas.value or 0
            except AttributeError:
                bucket_data['total_roas'] = 0

            response[bucket_date] = bucket_data

        log.info(
            f'Queried SA objectives',
        )

        return response

    def sales_and_spend(self, api, start_date, end_date):
        log.info(
            f'Querying sales and spend...',
        )

        response = {
            'dsp_sales': 0,
            'dsp_spend': 0,
            'dsp_total_sales': 0,
            'sa_sales': 0,
            'sa_spend': 0,
            'sa_total_sales': 0,
        }
        
        try:
            aggregate = sales_and_spend_aggregation(
                self._client,
                self._index(api),
                start_date,
                end_date,
            )
        except Exception as e:
            log.exception(e)
            return response

        response = {
            'dsp_sales': aggregate.aggregations.dsp_sales.value,
            'dsp_spend': aggregate.aggregations.dsp_spend.value,
            'dsp_total_sales': aggregate.aggregations.dsp_total_sales.value,
            'sa_sales': aggregate.aggregations.sa_sales.value,
            'sa_spend': aggregate.aggregations.sa_spend.value,
            'sa_total_sales': aggregate.aggregations.sa_total_sales.value,
        }

        log.info(
            f'Queried sales and spend',
        )

        return response
    
    def sales_and_spend_by_objective(self, api, start_date, end_date):
        log.info(
            f'Querying sales and spend by objective...',
        )

        previous_period = self.date_utility.previous_period(
            start_date,
            end_date,
        )
        previous_period_start_date = previous_period.get(
            Constants.PREVIOUS_PERIOD_START_DATE,
        )
        previous_period_end_date = previous_period.get(
            Constants.PREVIOUS_PERIOD_END_DATE,
        )

        adjusted_previous_period_end_date = previous_period_end_date + timedelta(days=1)
        adjusted_end_date = end_date + timedelta(days=1)

        formatted_start_date = self._date_utility.to_string(
            start_date,
        )
        formatted_end_date = self._date_utility.to_string(
            adjusted_end_date,
        )
        formatted_previous_start_date = self._date_utility.to_string(
            previous_period_start_date,
        )
        formatted_previous_end_date = self._date_utility.to_string(
            adjusted_previous_period_end_date,
        )

        response = {
            'current_time_period': {
                'dsp_total_attributed_sales': 0,
                'dsp_total_sales': 0,
                'dsp_total_spend': 0,
                'sa_total_attributed_sales': 0,
                'sa_total_sales': 0,
                'sa_total_spend': 0,
                'total_attributed_sales': 0,
                'total_sales': 0,
                'total_spend': 0,
            },
            'objectives': {objective.value: {
                'dsp_sales': 0,
                'dsp_spend': 0,
                'dsp_total_sales': 0,
                'sa_sales': 0,
                'sa_spend': 0,
                'sa_total_sales': 0,
            } for _, objective in ObjectiveType.__members__.items()},
            'period': {
                'start_date': self.date_utility.to_string(
                    previous_period_start_date,
                    Constants.DATE_FORMAT_MM__DD__YYYY,
                ),
                'end_date': self.date_utility.to_string(
                    previous_period_end_date,
                    Constants.DATE_FORMAT_MM__DD__YYYY,
                ),
            },
            'previous_time_period': {
                'dsp_total_attributed_sales': 0,
                'dsp_total_sales': 0,
                'dsp_total_spend': 0,
                'sa_total_attributed_sales': 0,
                'sa_total_sales': 0,
                'sa_total_spend': 0,
                'total_attributed_sales': 0,
                'total_sales': 0,
                'total_spend': 0,
            },
            'dsp_total_sales_diff': 0,
            'dsp_total_spend_diff': 0,
            'sa_total_sales_diff': 0,
            'sa_total_spend_diff': 0,
            'total_attributed_sales_diff': 0,
            'total_sales_diff': 0,
            'total_spend_diff': 0,
        }

        try:
            aggregate = sales_and_spend_by_objective_aggregation(
                self._client,
                self._index(api),
                formatted_start_date,
                formatted_end_date,
                formatted_previous_start_date,
                formatted_previous_end_date,
            )
        except Exception as e:
            log.exception(e)
            return response
        
        objectives = [objective.value for objective in ObjectiveType]

        current_period_buckets = aggregate.aggregations.current_period.buckets
        previous_period_buckets = aggregate.aggregations.previous_period.buckets

        for bucket in current_period_buckets:
            objective_buckets = bucket.objectives.buckets
            
            for objective in objective_buckets:
                try:
                    response['objectives'][objective.key]['dsp_sales'] = objective.dsp_sales.value or 0
                except AttributeError:
                    response['objectives'][objective.key]['dsp_sales'] = 0

                try:
                    response['objectives'][objective.key]['dsp_spend'] = objective.dsp_spend.value or 0
                except AttributeError:
                    response['objectives'][objective.key]['dsp_spend'] = 0

                try:
                    response['objectives'][objective.key]['dsp_total_sales'] = objective.dsp_total_sales.value or 0
                except AttributeError:
                    response['objectives'][objective.key]['dsp_total_sales'] = 0

                try:
                    response['objectives'][objective.key]['sa_sales'] = objective.sa_sales.value or 0
                except AttributeError:
                    response['objectives'][objective.key]['sa_sales'] = 0

                try:
                    response['objectives'][objective.key]['sa_spend'] = objective.sa_spend.value or 0
                except AttributeError:
                    response['objectives'][objective.key]['sa_spend'] = 0

                try:
                    response['objectives'][objective.key]['sa_total_sales'] = objective.sa_total_sales.value or 0
                except AttributeError:
                    response['objectives'][objective.key]['sa_total_sales'] = 0

            for bucket in current_period_buckets:
                try:
                    response['current_time_period']['dsp_total_attributed_sales'] = bucket.dsp_total_attributed_sales.value or 0
                except AttributeError:
                    response['current_time_period']['dsp_total_attributed_sales'] = 0

                try:
                    response['current_time_period']['dsp_total_sales'] = bucket.dsp_total_sales.value or 0
                except AttributeError:
                    response['current_time_period']['dsp_total_sales'] = 0
                
                try:
                    response['current_time_period']['dsp_total_spend'] = bucket.dsp_total_spend.value or 0
                except AttributeError:
                    response['current_time_period']['dsp_total_spend'] = 0

                try:
                    response['current_time_period']['sa_total_attributed_sales'] = bucket.sa_total_attributed_sales.value or 0
                except AttributeError:
                    response['current_time_period']['sa_total_attributed_sales'] = 0

                try:
                    response['current_time_period']['sa_total_sales'] = bucket.sa_total_sales.value or 0
                except AttributeError:
                    response['current_time_period']['sa_total_sales'] = 0
                
                try:
                    response['current_time_period']['sa_total_spend'] = bucket.sa_total_spend.value or 0
                except AttributeError:
                    response['current_time_period']['sa_total_spend'] = 0
                
                try:
                    response['current_time_period']['total_attributed_sales'] = bucket.total_attributed_sales.value or 0
                except AttributeError:
                    response['current_time_period']['total_attributed_sales'] = 0

                try:
                    response['current_time_period']['total_sales'] = bucket.total_sales.value or 0
                except AttributeError:
                    response['current_time_period']['total_sales'] = 0
                
                try:
                    response['current_time_period']['total_spend'] = bucket.total_spend.value or 0
                except AttributeError:
                    response['current_time_period']['total_spend'] = 0

            for bucket in previous_period_buckets:
                try:
                    response['previous_time_period']['dsp_total_attributed_sales'] = bucket.dsp_total_attributed_sales.value or 0
                except AttributeError:
                    response['previous_time_period']['dsp_total_attributed_sales'] = 0

                try:
                    response['previous_time_period']['dsp_total_sales'] = bucket.dsp_total_sales.value or 0
                except AttributeError:
                    response['previous_time_period']['dsp_total_sales'] = 0
                
                try:
                    response['previous_time_period']['dsp_total_spend'] = bucket.dsp_total_spend.value or 0
                except AttributeError:
                    response['previous_time_period']['dsp_total_spend'] = 0

                try:
                    response['previous_time_period']['sa_total_attributed_sales'] = bucket.sa_total_attributed_sales.value or 0
                except AttributeError:
                    response['previous_time_period']['sa_total_attributed_sales'] = 0

                try:
                    response['previous_time_period']['sa_total_sales'] = bucket.sa_total_sales.value or 0
                except AttributeError:
                    response['previous_time_period']['sa_total_sales'] = 0
                
                try:
                    response['previous_time_period']['sa_total_spend'] = bucket.sa_total_spend.value or 0
                except AttributeError:
                    response['previous_time_period']['sa_total_spend'] = 0

                try:
                    response['previous_time_period']['total_attributed_sales'] = bucket.total_attributed_sales.value or 0
                except AttributeError:
                    response['previous_time_period']['total_attributed_sales'] = 0
                
                try:
                    response['previous_time_period']['total_sales'] = bucket.total_sales.value or 0
                except AttributeError:
                    response['previous_time_period']['total_sales'] = 0
                
                try:
                    response['previous_time_period']['total_spend'] = bucket.total_spend.value or 0
                except AttributeError:
                    response['previous_time_period']['total_spend'] = 0
            
        response['dsp_total_sales_diff'] = self.data_utility.delta(
            response['current_time_period']['dsp_total_sales'],
            response['previous_time_period']['dsp_total_sales'],
        )
        response['dsp_total_spend_diff'] = self.data_utility.delta(
            response['current_time_period']['dsp_total_spend'],
            response['previous_time_period']['dsp_total_spend'],
        )
        response['sa_total_sales_diff'] = self.data_utility.delta(
            response['current_time_period']['sa_total_sales'],
            response['previous_time_period']['sa_total_sales'],
        )
        response['sa_total_spend_diff'] = self.data_utility.delta(
            response['current_time_period']['sa_total_spend'],
            response['previous_time_period']['sa_total_spend'],
        )
        response['total_attributed_sales_diff'] = self.data_utility.delta(
            response['current_time_period']['total_attributed_sales'],
            response['previous_time_period']['total_attributed_sales'],
        )
        response['total_sales_diff'] = self.data_utility.delta(
            response['current_time_period']['total_sales'],
            response['previous_time_period']['total_sales'],
        )
        response['total_spend_diff'] = self.data_utility.delta(
            response['current_time_period']['total_spend'],
            response['previous_time_period']['total_spend'],
        )

        log.info(
            f'Queried sales and spend by objective',
        )

        return response

    def total_attributed_sales_and_spend(self, api, start_date, end_date):
        log.info(
            f'Querying total attributed sales and spend...',
        )

        previous_period = self.date_utility.previous_period(
            start_date,
            end_date,
        )
        previous_period_start_date = previous_period.get(
            Constants.PREVIOUS_PERIOD_START_DATE,
        )
        previous_period_end_date = previous_period.get(
            Constants.PREVIOUS_PERIOD_END_DATE,
        )

        response = {
            'previous_time_period': {
                'total_attributed_sales': 0,
                'total_sales': 0,
                'total_spend': 0,
                'period': {
                    'start_date': self.date_utility.to_string(
                        previous_period_start_date,
                        Constants.DATE_FORMAT_MM__DD__YYYY,
                    ),
                    'end_date': self.date_utility.to_string(
                        previous_period_end_date,
                        Constants.DATE_FORMAT_MM__DD__YYYY,
                    ),
                },
            },
            'sponsored_ads': {
                'dsp': {
                    'sales': 0,
                    'spend': 0,
                    'total_sales': 0,
                },
                'sb': {
                    'sales': 0,
                    'spend': 0,
                    'total_sales': 0,
                },
                'sd': {
                    'sales': 0,
                    'spend': 0,
                    'total_sales': 0,
                },
                'sp': {
                    'sales': 0,
                    'spend': 0,
                    'total_sales': 0,
                },
                'total_attributed_sales': 0,
                'total_sales': 0,
                'total_spend': 0,
            },
            'sales_diff': 0,
            'spend_diff': 0,
            'total_sales_diff': 0,
        }
        
        try:
            aggregate = total_attributed_sales_and_spend_aggregation(
                self._client,
                self._index(api),
                self.date_utility.to_string(previous_period_start_date),
                self.date_utility.to_string(previous_period_end_date),
            )
            
            detail_aggregate = total_attributed_sales_and_spend_detail_aggregation(
                self._client,
                self._index(api),
                self.date_utility.to_string(start_date),
                self.date_utility.to_string(end_date),
            )
        except Exception as e:
            log.exception(e)
            return response

        response = {
            'previous_time_period': {
                'total_attributed_sales': aggregate.aggregations.total_attributed_sales.value,
                'total_sales': aggregate.aggregations.total_sales.value,
                'total_spend': aggregate.aggregations.total_spend.value,
                'period': {
                    'start_date': self.date_utility.to_string(
                        previous_period_start_date,
                        Constants.DATE_FORMAT_MM__DD__YYYY,
                    ),
                    'end_date': self.date_utility.to_string(
                        previous_period_end_date,
                        Constants.DATE_FORMAT_MM__DD__YYYY,
                    ),
                },
            },
            'sponsored_ads': {
                'dsp': {
                    'sales': 0,
                    'spend': 0,
                    'total_sales': 0,
                },
                'sb': {
                    'sales': 0,
                    'spend': 0,
                    'total_sales': 0,
                },
                'sd': {
                    'sales': 0,
                    'spend': 0,
                    'total_sales': 0,
                },
                'sp': {
                    'sales': 0,
                    'spend': 0,
                    'total_sales': 0,
                }
            },
        }
        
        aggregations = detail_aggregate.aggregations
        for bucket in aggregations.sponsored_ads.buckets:
            response['sponsored_ads'][bucket.key] = {
                'sales': bucket.sales.value,
                'spend': bucket.spend.value,
                'total_sales': bucket.total_sales.value,
            }

        response['sponsored_ads']['total_attributed_sales'] = aggregations.total_attributed_sales.value
        response['sponsored_ads']['total_sales'] = aggregations.total_sales.value
        response['sponsored_ads']['total_spend'] = aggregations.total_spend.value

        response['sales_diff'] = self.data_utility.delta(
            response['sponsored_ads']['total_sales'],
            response['previous_time_period']['total_sales'],
        )

        response['spend_diff'] = self.data_utility.delta(
            response['sponsored_ads']['total_spend'],
            response['previous_time_period']['total_spend'],
        )

        response['total_sales_diff'] = self.data_utility.delta(
            response['sponsored_ads']['total_attributed_sales'],
            response['previous_time_period']['total_attributed_sales'],
        )
        
        log.info(
            f'Queried total attributed sales and spend',
        )

        return response

    # Retail

    def brand_analytics(self, asin_metadata, distributor_view, report_type, selling_program, from_date, to_date, interval):
        log.info(
            f'Querying brand analytics...',
        )

        if from_date and to_date:
            from_date, to_date = self.date_utility.to_es_dates(
                from_date,
                to_date,
            )

            from_date = self.date_utility.to_string(from_date)
            to_date = self.date_utility.to_string(to_date)

        response = {
            'data': {},
        }

        try:
            time_series = brand_analytics_time_series(
                self._client,
                self._ba_index(),
                distributor_view,
                report_type,
                selling_program,
                from_date,
                to_date,
                interval,
            )
        except Exception as e:
            print(e)
            log.exception(e)
            return response

        buckets = time_series.aggregations.interval.buckets
        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )

            response['data'][bucket_date] = {
                'asins': [],
            }

            asins = bucket.asins.buckets
            for asin_bucket in asins:
                asin = asin_bucket.key

                asin_metadatum = asin_metadata.get(asin, {})
                
                response['data'][bucket_date]['asins'].append({
                    'asin': asin,
                    'brand': asin_metadatum.get('brand', Constants.NO_BRAND),
                    'category': asin_metadatum.get('category', Constants.NO_CATEGORY),
                    'name': asin_metadatum.get('name', Constants.NO_NAME),
                    'glanceViews': asin_bucket.glanceViews.value or 0,
                    'orderedRevenue': asin_bucket.orderedRevenue.value or 0,
                    'orderedUnits': asin_bucket.orderedUnits.value or 0,
                    'shippedCOGS': asin_bucket.shippedCOGS.value or 0,
                    'shippedRevenue': asin_bucket.shippedRevenue.value or 0,
                    'shippedUnits': asin_bucket.shippedUnits.value or 0,
                })

        log.info(
            f'Queried brand analytics',
        )

        return response

    def brand_analytics_statistics(self, asin_metadata, distributor_view, report_type, selling_program, from_date, to_date):
        log.info(
            f'Querying brand analytics statistics...',
        )

        if from_date and to_date:
            from_date, to_date = self.date_utility.to_es_dates(
                from_date,
                to_date,
            )

            from_date = self.date_utility.to_string(from_date)
            to_date = self.date_utility.to_string(to_date)

        response = {
            'data': {},
        }

        try:
            statistics = brand_analytics_statistics(
                self._client,
                self._ba_index(),
                distributor_view,
                report_type,
                selling_program,
                from_date,
                to_date,
            )
        except Exception as e:
            print(e)
            log.exception(e)
            return response

        buckets = statistics.aggregations.asins.buckets
        for bucket in buckets:
            asin = bucket.key

            asin_metadatum = asin_metadata.get(asin, {})
            
            response['data'][asin] = {
                'asin': asin,
                'brand': asin_metadatum.get('brand', Constants.NO_BRAND),
                'category': asin_metadatum.get('category', Constants.NO_CATEGORY),
                'name': asin_metadatum.get('name', Constants.NO_NAME),
                'glanceViews': bucket.glanceViews.value or 0,
                'orderedRevenue': bucket.orderedRevenue.value or 0,
                'orderedUnits': bucket.orderedUnits.value or 0,
                'shippedCOGS': bucket.shippedCOGS.value or 0,
                'shippedRevenue': bucket.shippedRevenue.value or 0,
                'shippedUnits': bucket.shippedUnits.value or 0,
            }

        log.info(
            f'Queried brand analytics statistics',
        )

        return response
    
    def search_terms_filter(self, query, limit):
        log.info(
            'Querying search term filter...',
        )

        response = []
        
        try:
            results = search_terms_filter(
                self._client,
                Constants.SEARCH_TERMS_INDEX,
                query,
                limit,
            )
        except Exception as e:
            log.exception(e)
            return response

        hits = results.hits.hits

        response = [
            hit._source.search_term for hit in hits
        ]

        log.info(
            'Queried search term filter',
        )

        return response        

    def search_term_periods(self):
        log.info(
            'Querying search term periods...',
        )

        response = set()

        try:
            periods = search_terms_periods(
                self._client,
                index=Constants.SEARCH_TERMS_INDEX,
            )
        except Exception as e:
            log.exception(e)
            return response

        buckets = periods.aggregations.dates.buckets
        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.SEARCH_TERMS_PERIODS_DATE_FORMAT,
            )
            response.add(bucket_date)

        log.info(
            'Queried search term periods',
        )    

        return response

    def search_terms_rank(self, data):
        log.info(
            'Querying search terms rank...',
        )

        response = {}

        try:
            search_terms_ranking = search_terms_time_series(
                self._client,
                Constants.SEARCH_TERMS_INDEX,
                data,
            )
        except Exception as e:
            log.exception(e)
            return response

        aggregations = search_terms_ranking.aggregations
        search_terms = list(aggregations.to_dict().keys())
        
        for search_term in search_terms:
            aggregation = aggregations[search_term]
            
            for bucket in aggregation.ranking_over_time.buckets:
                start_date = self.date_utility.timestamp_to_date(
                    bucket.key / 1000,
                )

                end_date = start_date + timedelta(days=7)

                start_date = self.date_utility.to_string(
                    start_date,
                    Constants.DATE_FORMAT_YYYY_MM_DD,
                )

                end_date = self.date_utility.to_string(
                    end_date,
                    Constants.DATE_FORMAT_YYYY_MM_DD,
                )

                try:
                    rank = bucket.ranking.buckets[0].key
                except IndexError:
                    rank = -1
                
                response[start_date] = response.get(
                    start_date,
                    {
                        'end_date': end_date,
                        'search_terms': {
                            search_term: -1 for search_term in search_terms
                        },
                        'start_date': start_date,
                    }
                )

                response[start_date]['search_terms'][search_term] = rank

        response = list(response.values())

        for item in response:
            item['search_terms'] = [
                { 'search_term': search_term, 'rank': rank, }
                for search_term, rank in item['search_terms'].items()
            ]

        log.info(
            'Queried search terms rank',
        )

        return response 

    def search_terms(self, data):
        log.info(
            'Querying search terms...',
        )

        response = []

        try:
            terms = search_terms(
                self._client,
                Constants.SEARCH_TERMS_INDEX,
                data,
            )
        except Exception as e:
            log.exception(e)
            return response

        hits = terms.hits.hits
        
        for hit in hits:
            try:
                response.append(IndexSearchTermsResponseSchema(**hit._source.to_dict()))
            except Exception as e:
                log.exception(e)
            
        log.info(
            'Queried search terms',
        )

        return response

    # Tags

    def dsp_and_sa_tags(self, campaign_ids, order_ids, start_date, end_date, interval, objectives, segments):
        log.info(
            f'Querying DSP and SA tags...',
        )

        response = {}

        try:
            time_series = dsp_and_sa_tags_time_series(
                self._client,
                self._indices(),
                campaign_ids,
                order_ids,
                start_date,
                end_date,
                interval,
                objectives,
                segments,
            )
        except Exception as e:
            log.exception(e)
            return response

        buckets = time_series.aggregations.interval.buckets
        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )

            bucket_data = {
                'dsp_clicks': bucket.dsp_clicks.value or 0,
                'dsp_dpv': bucket.dsp_dpv.value or 0,
                'dsp_sales': bucket.dsp_sales.value or 0,
                'dsp_spend': bucket.dsp_spend.value or 0,
                'dsp_total_ntb_sales': bucket.dsp_total_ntb_sales.value or 0,
                'dsp_total_product_sales': bucket.dsp_total_product_sales.value or 0,
                'dsp_purchases_14d': bucket.dsp_purchases_14d.value or 0,
                'dsp_dpvr': bucket.dsp_dpvr.value or 0,
                'dsp_impressions': bucket.dsp_impressions.value or 0,
                'dsp_total_units_sold': bucket.dsp_total_units_sold.value or 0,
                'dsp_units_sold': bucket.dsp_units_sold.value or 0,
                'dsp_ecpm': bucket.dsp_ecpm.value or 0,
                'sa_clicks': bucket.sa_clicks.value or 0,
                'sa_sales': bucket.sa_sales.value or 0,
                'sa_spend': bucket.sa_spend.value or 0,
                'sa_total_ntb_sales': bucket.sa_total_ntb_sales.value or 0,
                'sa_total_product_sales': bucket.sa_total_product_sales.value or 0,
                'sa_attributed_conversions': bucket.sa_attributed_conversions_14d.value or 0,
                'sa_impressions': bucket.sa_impressions.value or 0,
                'sa_units_sold': bucket.sa_units_sold.value or 0,
                'sa_total_units_sold': bucket.sa_total_units_sold.value or 0,
            }

            try:
                bucket_data['acpc'] = bucket.cpa.value or 0
            except AttributeError:
                bucket_data['acpc'] = 0

            try:
                bucket_data['cpa'] = bucket.cpa.value or 0
            except AttributeError:
                bucket_data['cpa'] = 0

            try:
                bucket_data['ctr'] = bucket.ctr.value or 0
            except AttributeError:
                bucket_data['ctr'] = 0

            try:
                bucket_data['dpvr'] = bucket.dpvr.value or 0
            except AttributeError:
                bucket_data['dpvr'] = 0

            try:
                bucket_data['ecpm'] = bucket.ecpm.value or 0
            except AttributeError:
                bucket_data['ecpm'] = 0

            try:
                bucket_data['ntb_percentage'] = bucket.ntb_percentage.value or 0
            except AttributeError:
                bucket_data['ntb_percentage'] = 0

            try:
                bucket_data['roas'] = bucket.roas.value or 0
            except AttributeError:
                bucket_data['roas'] = 0

            try:
                bucket_data['total_impressions'] = bucket.total_impressions.value or 0
            except AttributeError:
                bucket_data['total_impressions'] = 0

            try:
                bucket_data['total_ntb_percentage'] = bucket.total_ntb_percentage.value or 0
            except AttributeError:
                bucket_data['total_ntb_percentage'] = 0

            try:
                bucket_data['total_roas'] = bucket.total_roas.value or 0
            except AttributeError:
                bucket_data['total_roas'] = 0

            response[bucket_date] = bucket_data

        log.info(
            f'Queried DSP and SA tags',
        )

        return response

    def dsp_tags(self, order_ids, start_date, end_date, interval, objectives, segments):
        log.info(
            f'Querying DSP tags...',
        )

        response = {}

        try:
            time_series = dsp_tags_time_series(
                self._client,
                self._dsp_index(),
                order_ids,
                start_date,
                end_date,
                interval,
                objectives,
                segments,
            )
        except Exception as e:
            log.exception(e)
            return response

        buckets = time_series.aggregations.interval.buckets
        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )

            bucket_data = {
                'clicks': bucket.clicks.value or 0,
                'dpv': bucket.dpv.value or 0,
                'dpvr': bucket.dpvr.value or 0,
                'ecpm': bucket.ecpm.value or 0,
                'impressions': bucket.impressions.value or 0,
                'ntb_percentage': bucket.ntb_percentage.value or 0,
                'sales': bucket.sales.value or 0,
                'spend': bucket.spend.value or 0,
                'total_ntb_percentage': bucket.total_ntb_percentage.value or 0,
                'total_ntb_sales': bucket.total_ntb_sales.value or 0,
                'total_product_sales': bucket.total_product_sales.value or 0,
                'purchases': bucket.purchases.value or 0,
                'units_sold': bucket.units_sold.value or 0,
                'total_units_sold': bucket.total_units_sold.value or 0,
            }

            try:
                bucket_data['acpc'] = bucket.cpa.value or 0
            except AttributeError:
                bucket_data['acpc'] = 0

            try:
                bucket_data['cpa'] = bucket.cpa.value or 0
            except AttributeError:
                bucket_data['cpa'] = 0

            try:
                bucket_data['ctr'] = bucket.ctr.value or 0
            except AttributeError:
                bucket_data['ctr'] = 0

            try:
                bucket_data['roas'] = bucket.roas.value or 0
            except AttributeError:
                bucket_data['roas'] = 0

            try:
                bucket_data['total_roas'] = bucket.total_roas.value or 0
            except AttributeError:
                bucket_data['total_roas'] = 0

            response[bucket_date] = bucket_data

        log.info(
            f'Queried DSP tags',
        )

        return response

    def sa_tags(self, campaign_ids, start_date, end_date, interval, objectives):
        log.info(
            f'Querying SA tags...',
        )

        response = {}

        try:
            time_series = sa_tags_time_series(
                self._client,
                self._sa_index(),
                campaign_ids,
                start_date,
                end_date,
                interval,
                objectives,
            )
        except Exception as e:
            log.exception(e)
            return response

        buckets = time_series.aggregations.interval.buckets
        for bucket in buckets:
            bucket_date = self.date_utility.timestamp_to_date(
                bucket.key / 1000,
            )
            bucket_date = self.date_utility.to_string(
                bucket_date,
                Constants.DATE_FORMAT_YYYY_MM_DD,
            )

            bucket_data = {
                'clicks': bucket.clicks.value or 0,
                'dpv': bucket.dpv.value or 0,
                'impressions': bucket.impressions.value or 0,
                'sales': bucket.sales.value or 0,
                'spend': bucket.spend.value or 0,
                'total_ntb_sales': bucket.total_ntb_sales.value or 0,
                'total_product_sales': bucket.total_product_sales.value or 0,
                'conversions': bucket.conversions.value or 0,
                'units_sold': bucket.units_sold.value or 0,
                'total_units_sold': bucket.total_units_sold.value or 0,
            }

            try:
                bucket_data['acpc'] = bucket.cpa.value or 0
            except AttributeError:
                bucket_data['acpc'] = 0

            try:
                bucket_data['cpa'] = bucket.cpa.value or 0
            except AttributeError:
                bucket_data['cpa'] = 0

            try:
                bucket_data['ctr'] = bucket.ctr.value or 0
            except AttributeError:
                bucket_data['ctr'] = 0

            try:
                bucket_data['dpvr'] = bucket.dpvr.value or 0
            except AttributeError:
                bucket_data['dpvr'] = 0

            try:
                bucket_data['ecpm'] = bucket.ecpm.value or 0
            except AttributeError:
                bucket_data['ecpm'] = 0

            try:
                bucket_data['roas'] = bucket.roas.value or 0
            except AttributeError:
                bucket_data['roas'] = 0

            try:
                bucket_data['total_roas'] = bucket.total_roas.value or 0
            except AttributeError:
                bucket_data['total_roas'] = 0

            response[bucket_date] = bucket_data

        log.info(
            f'Queried SA tags',
        )

        return response

    def tag_statistics(self, campaign_ids, order_ids, start_date, end_date, objectives, segments):
        log.info(
            f'Querying DSP and SA tag statistics',
        )
        
        responses = []

        try:
            time_series = tag_statistics(
                self._client,
                self._indices(),
                campaign_ids,
                order_ids,
                start_date,
                end_date,
                objectives,
                segments,
            )
        except Exception as e:
            log.exception(e)
            return responses

        buckets = time_series.aggregations.campaigns.buckets
        for bucket in buckets:
            response = {
                'name': bucket.key,
                'sales': bucket.sales.value or 0,
                'spend': bucket.spend.value or 0,
                'clicks': bucket.clicks.value or 0,
                'impressions': bucket.impressions.value or 0,
                'total_sales': bucket.total_sales.value or 0,
            }

            responses.append(response)

        log.info(
            f'Queried DSP and SA tag statistics',
        )

        return responses

    @property
    def amazon(self):
        return self._brand_manager.brand.amazon
    
    @property
    def data_utility(self):
        if self._data_utility is None:
            self._data_utility = DataUtility()

        return self._data_utility
    
    @property
    def date_utility(self):
        if self._date_utility is None:
            self._date_utility = DateUtility()

        return self._date_utility

    def _ba_index(self):
        try:
            vendor_id = self.amazon.aa.sp.seller_partner_id.split(
                Constants.PERIOD,
            )[-1]
            return f'{Constants.BRAND_ANALYTICS_INDEX}_{vendor_id}'
        except KeyError:
            return None


    def _dsp_index(self):
        try:
            return f'{Constants.DSP_INDEX}_{self.amazon.aa.dsp.advertiser_id}'
        except KeyError:
            return None
        
    def _index(self, api):
        if api == ApiType.DSP:
            return f'{Constants.DSP_INDEX}_{self.amazon.aa.dsp.advertiser_id}'
        elif api == ApiType.SA:
            return f'{Constants.SPONSORED_ADS_INDEX}_{self.amazon.aa.sa.advertiser_id}'

    def _indices(self):
        indices = [self._sa_index(), self._dsp_index()]
        return [index for index in indices if index]

    def _sa_index(self):
        try:
            return f'{Constants.SPONSORED_ADS_INDEX}_{self.amazon.aa.sa.advertiser_id}'
        except KeyError:
            return None


if __name__ == '__main__':
    import json
    from server.resources.schema.amazon_api import (
        IndexSearchTermsRankingSchema,
        IndexSearchTermsSchema,
    )
    from server.resources.types.data_types import (
        BrandAnalyticsDistributorType,
        BrandAnalyticsSellingProgramType,
        BrandAnalyticsReportType,
        BrandAnalyticsSalesType,
    )
    from server.services.aws_service import AWSService
    aws_service = AWSService()
    es_service = aws_service.es_service
    ssm_service = aws_service.ssm_service
    # es_service.domain = ssm_service.amazon_advertising_elasticsearch_domain
    es_service.domain = ssm_service.amazon_retail_elasticsearch_domain
    client = es_service.es_service

    data_service = DataService(client)

    results = data_service.dsp_and_sa_objectives(
        'dsp',
        '2021-07-01',
        '2021-09-30',
        'week',
        ['BP'],
        [],
    )

    print(
        json.dumps(
            results,
            indent=2,
            sort_keys=False,
        )
    )