import calendar
import datetime

from elasticsearch_dsl import (
    A,
    Q,
    Search,
)

from server.core.constants import Constants
from server.resources.types.data_types import (
    BrandAnalyticsIntervalType,
    TableType,
)
from server.utilities.data_utility import DataUtility


data_utility = DataUtility()

# Advertising

def advertising_sales_and_total_sales_time_series(client, index, start_date, end_date, interval):
    search = Search(
        using=client,
        index=index,
    )

    query = Q(
        'bool',
        minimum_should_match=1,
        should=[
            Q(
                'term',
                type={
                    'value': 'campaign',
                },
            ),
            Q(
                'term',
                type={
                    'value': 'campaigns',
                },
            ),
        ],
        must=[
            Q(
                'bool',
                minimum_should_match=1,
                should=[
                    Q(
                        'term',
                        segment={
                            'value': 'null',
                        },
                    ),
                    Q(
                        'bool',
                        minimum_should_match=1,
                        should=[
                            Q(
                                'bool',
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                    Q(
                                        'exists',
                                        field='dimension',
                                    ),
                                ],
                            ),
                            Q(
                                'bool',
                                must=[
                                    Q(
                                        'term',
                                        dimension={
                                            'value': 'order',
                                        },
                                    ),
                                ],
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                ]
                            )
                        ]
                    )
                ]
            ),
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        ],
    )

    search = search.query(query)

    search.aggs.bucket(
        'interval',
        'date_histogram',
        extended_bounds={
            'min': start_date,
            'max': end_date,
        },
        field='report_date',
        interval=interval,
        min_doc_count=0,
    )
    search.aggs['interval'].metric(
        'dsp_advertising_sales',
        'sum',
        field='sales_14d',
    ).metric(
        'dsp_total_sales',
        'sum',
        field='total_sales_14d',
    ).metric(
        'sa_advertising_sales',
        'sum',
        field='attributed_sales_14d_same_SKU',
    ).metric(
        'sa_total_sales',
        'sum',
        field='attributed_sales_14d',
    ).metric(
        'advertising_percent',
        'bucket_script',
        buckets_path={
            'dsp_advertising_sales': 'dsp_advertising_sales',
            'dsp_total_sales': 'dsp_total_sales',
            'sa_advertising_sales': 'sa_advertising_sales',
            'sa_total_sales': 'sa_total_sales',
        },
        script='((params.sa_advertising_sales + params.dsp_advertising_sales)/(params.sa_total_sales + params.dsp_total_sales)) * 100',
    )

    return search.execute()


def advertising_statistics(client, index, start_date, end_date, interval, table):
    search = Search(
        using=client,
        index=index,
    )

    if table == TableType.SALES_AND_SPEND:
        query = Q(
            'bool',
            minimum_should_match=1,
            should=[
                Q(
                    'term',
                    type={
                        'value': 'campaign',
                    },
                ),
                Q(
                    'term',
                    type={
                        'value': 'campaigns',
                    },
                ),
            ],
            must=[
                Q(
                    'bool',
                    minimum_should_match=1,
                    should=[
                        Q(
                            'term',
                            segment={
                                'value': 'null',
                            },
                        ),
                        Q(
                            'bool',
                            minimum_should_match=1,
                            should=[
                                Q(
                                    'bool',
                                    must_not=[
                                        Q(
                                            'exists',
                                            field='segment',
                                        ),
                                        Q(
                                            'exists',
                                            field='dimension',
                                        ),
                                    ],
                                ),
                                Q(
                                    'bool',
                                    must=[
                                        Q(
                                            'term',
                                            dimension={
                                                'value': 'order',
                                            },
                                        ),
                                    ],
                                    must_not=[
                                        Q(
                                            'exists',
                                            field='segment',
                                        ),
                                    ]
                                )
                            ]
                        )
                    ]
                ),
                Q(
                    'range',
                    report_date={
                        'gte': start_date,
                        'lte': end_date,
                    },
                ),
            ],
        )

        search = search.query(query)

        search.aggs.bucket(
            'interval',
            'date_histogram',
            extended_bounds={
                'min': start_date,
                'max': end_date,
            },
            field='report_date',
            interval=interval,
            min_doc_count=0,
        )
        search.aggs['interval'].metric(
            'dsp_sales',
            'sum',
            field='sales_14d',
        ).metric(
            'dsp_spend',
            'sum',
            field='total_cost',
        ).metric(
            'dsp_total_sales',
            'sum',
            field='total_sales_14d',
        ).metric(
            'sa_sales',
            'sum',
            field='attributed_sales_14d_same_SKU',
        ).metric(
            'sa_spend',
            'sum',
            field='cost',
        ).metric(
            'sa_total_sales',
            'sum',
            field='attributed_sales_14d',
        )
    else:
        query = Q(
            'bool',
            must=[
                Q(
                    'bool',
                    minimum_should_match=1,
                    should=[
                        Q(
                            'term',
                            type={
                                'value': 'campaign',
                            },
                        ),
                        Q(
                            'term',
                            type={
                                'value': 'campaigns',
                            }
                        ),
                    ],
                    must=[
                        Q(
                            'bool',
                            minimum_should_match=1,
                            should=[
                                Q(
                                    'term',
                                    segment={
                                        'value': 'null',
                                    },
                                ),
                                Q(
                                    'bool',
                                    minimum_should_match=1,
                                    should=[
                                        Q(
                                            'bool',
                                            must_not=[
                                                Q(
                                                    'exists',
                                                    field='segment',
                                                ),
                                                Q(
                                                    'exists',
                                                    field='dimension',
                                                ),
                                            ],
                                        ),
                                        Q(
                                            'bool',
                                            must=[
                                                Q(
                                                    'term',
                                                    dimension={
                                                        'value': 'order',
                                                    },
                                                ),
                                            ],
                                            must_not=[
                                                Q(
                                                    'exists',
                                                    field='segment',
                                                ),
                                            ],
                                        )
                                    ],
                                )
                            ],
                        ),
                    ],
                ),
                Q(
                    'range',
                    report_date={
                        'gte': start_date,
                        'lte': end_date,
                    },
                ),
            ],
        )

        search = search.query(query)

        search = search.filter(
            Q(
                'script',
                script={
                    'source': """
                        String type = doc.type.value;
                        if (type == "campaigns") {
                            String campaignName = doc['campaign_name'].value;

                            if (campaignName.length() < 2) { return false; }

                            String objective = campaignName.substring(0,2);
                            return params.objectives.contains(objective);
                        } else {
                            String name = doc['order_name'].value;
                        
                            int indexOfObjective = name.indexOf('[O]');
                            
                            if (indexOfObjective != -1) {
                                String objective = name.substring(indexOfObjective + 4, indexOfObjective + 6);
                                return params.objectives.contains(objective);
                            }
                        }
                        return false;
                    """,
                    'params': {
                        'objectives': ['AM', 'BP', 'CQ', 'DC', 'UZ', 'XM'],
                    },
                }
            )
        )

        search.aggs.bucket(
            'interval',
            'date_histogram',
            extended_bounds={
                'min': start_date,
                'max': end_date,
            },
            field='report_date',
            interval=interval,
            min_doc_count=0,
        )
        
        objectives_aggregation = A(
            'terms',
            size=10,
            script={
                'source': """
                    String type = doc.type.value;
                    if (type == "campaigns") {
                        String campaignName = doc['campaign_name'].value;

                        if (campaignName.length() < 2) { return ''; }

                        return campaignName.substring(0,2);
                    } else {
                        String name = doc['order_name'].value;
                        
                        int indexOfObjective = name.indexOf('[O]');
                        
                        if (indexOfObjective != -1) {
                            return name.substring(indexOfObjective + 4, indexOfObjective + 6);
                        }
                    }
                """,
            },
        ).metric(
            'dsp_sales',
            'sum',
            field='sales_14d',
        ).metric(
            'dsp_spend',
            'sum',
            field='total_cost',
        ).metric(
            'dsp_total_sales',
            'sum',
            field='total_sales_14d',
        ).metric(
            'sa_sales',
            'sum',
            field='attributed_sales_14d_same_SKU',
        ).metric(
            'sa_spend',
            'sum',
            field='cost',
        ).metric(
            'sa_total_sales',
            'sum',
            field='attributed_sales_14d',
        ).metric(
            'sales',
            'bucket_script',
            buckets_path={
                'dsp_sales': 'dsp_sales',
                'sa_sales': 'sa_sales',
            },
            script='params.dsp_sales + params.sa_sales',
        ).metric(
            'spend',
            'bucket_script',
            buckets_path={
                'dsp_spend': 'dsp_spend',
                'sa_spend': 'sa_spend',
            },
            script='params.dsp_spend + params.sa_spend',
        ).metric(
            'total_sales',
            'bucket_script',
            buckets_path={
                'dsp_sales': 'dsp_total_sales',
                'sa_sales': 'sa_total_sales',
            },
            script='params.dsp_sales + params.sa_sales',
        )

        search.aggs['interval'].bucket(
            'objectives',
            objectives_aggregation,
        )

    return search.execute()


def cumulative_sales_and_spend_time_series(client, index, start_date, end_date, interval):
    search = Search(
        using=client,
        index=index,
    )

    query = Q(
        'bool',
        minimum_should_match=1,
        must=[
            Q(
                'bool',
                minimum_should_match=1,
                should=[
                    Q(
                        'term',
                        segment={
                            'value': 'null',
                        },
                    ),
                    Q(
                        'bool',
                        minimum_should_match=1,
                        should=[
                            Q(
                                'bool',
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment'
                                    ),
                                    Q(
                                        'exists',
                                        field='dimension',
                                    ),
                                ],
                            ),
                            Q(
                                'bool',
                                must=[
                                    Q(
                                        'term',
                                        dimension={
                                            'value': 'order',
                                        },
                                    ),
                                ],
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        ],
        should=[
            Q(
                'term',
                type={
                    'value': 'campaign',
                },
            ),
            Q(
                'term',
                type={
                    'value': 'campaigns',
                },
            ),
        ]
    )

    search = search.query(query)

    search.aggs.bucket(
        'interval',
        'date_histogram',
        extended_bounds={
            'min': start_date,
            'max': end_date,
        },
        field='report_date',
        interval=interval,
        min_doc_count=0,
    )
    search.aggs['interval'].metric(
        'dsp_sales',
        'sum',
        field='sales_14d',
    ).metric(
        'dsp_spend',
        'sum',
        field='total_cost',
    ).metric(
        'dsp_total_sales',
        'sum',
        field='total_sales_14d',
    ).metric(
        'sa_sales',
        'sum',
        field='attributed_sales_14d_same_SKU',
    ).metric(
        'sa_spend',
        'sum',
        field='cost',
    ).metric(
        'sa_total_sales',
        'sum',
        field='attributed_sales_14d',
    ).metric(
        'roas',
        'bucket_script',
        buckets_path={
            'dsp_sales': 'dsp_sales',
            'dsp_spend': 'dsp_spend',
            'sa_sales': 'sa_sales',
            'sa_spend': 'sa_spend',
        },
        script='((params.sa_sales + params.dsp_sales)/(params.dsp_spend + params.sa_spend))',
        gap_policy='insert_zeros',
    ).metric(
        'total_roas',
        'bucket_script',
        buckets_path={
            'dsp_sales': 'dsp_total_sales',
            'dsp_spend': 'dsp_spend',
            'sa_sales': 'sa_total_sales',
            'sa_spend': 'sa_spend',
        },
        script='((params.sa_sales + params.dsp_sales)/(params.dsp_spend + params.sa_spend))',
        gap_policy='insert_zeros',
    )

    return search.execute()


def dsp_and_sa_objectives_time_series(client, index, start_date, end_date, interval, objectives, segments):
    search = Search(
        using=client,
        index=index,
    )

    query = Q(
        'bool',
        minimum_should_match=1,
        should=[
            Q(
                'term',
                type={
                    'value': 'campaign',
                },
            ),
            Q(
                'term',
                type={
                    'value': 'campaigns',
                },
            ),
        ],
        must=[
            Q(
                'bool',
                minimum_should_match=1,
                should=[
                    Q(
                        'term',
                        segment={
                            'value': 'null',
                        },
                    ),
                    Q(
                        'bool',
                        minimum_should_match=1,
                        should=[
                            Q(
                                'bool',
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                    Q(
                                        'exists',
                                        field='dimension',
                                    ),
                                ],
                            ),
                            Q(
                                'bool',
                                must=[
                                    Q(
                                        'term',
                                        dimension={
                                            'value': 'order',
                                        },
                                    ),
                                ],
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                ]
                            )
                        ],
                    ),
                ],
            ),
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        ]
    )

    search = search.query(query)

    if objectives or segments:
        search = search.filter(
            'script',
            script={
                'source': """
                    String type = doc.type.value;
                    
                    if (type == "campaigns") {
                        String campaignName = doc['campaign_name'].value;

                        if (campaignName.length() < 2) { return false; }

                        String objective = campaignName.substring(0,2);
                        return params.objectives.contains(objective);
                    } else {
                        String name = doc['order_name'].value;
                        
                        int indexOfFunnel = name.indexOf('[F]');
                        int indexOfObjective = name.indexOf('[O]');
                        
                        if(indexOfObjective != -1 || indexOfFunnel != -1) {
                            String objective = name.substring(indexOfObjective + 4, indexOfObjective + 6);
                            String funnel = name.substring(indexOfFunnel + 4, indexOfFunnel + 6);
                            return params.objectives.contains(objective) || params.funnels.contains(funnel);
                        }
                    }
                """,
                'params': {
                    'funnels': segments,
                    'objectives': objectives,
                },
            }
        )

    search.aggs.bucket(
        'interval',
        'date_histogram',
        extended_bounds={
            'min': start_date,
            'max': end_date,
        },
        field='report_date',
        interval=interval,
        min_doc_count=0,
    )
    search.aggs['interval'].metric(
        'dsp_clicks',
        'sum',
        field='click_throughs',
    ).metric(
        'acpc',
        'bucket_script',
        buckets_path={
            'dsp_clicks': 'dsp_clicks',
            'dsp_spend': 'dsp_spend',
            'sa_clicks': 'sa_clicks',
            'sa_spend': 'sa_spend',
        },
        script='(params.dsp_spend + params.sa_spend)/(params.dsp_clicks + params.sa_clicks)',
    ).metric(
        'dsp_dpv',
        'sum',
        field='dpv_14d',
    ).metric(
        'dsp_dpvr',
        'sum',
        field='dpvr_14d',
    ).metric(
        'dsp_ecpm',
        'sum',
        field='e_cpm',
    ).metric(
        'dsp_impressions',
        'sum',
        script={
            'source': """
                if (doc.type.value == 'campaign') return doc.impressions.value;
            """
        },
    ).metric(
        'dsp_purchases_14d',
        'sum',
        field='purchases_14d',
    ).metric(
        'dsp_sales',
        'sum',
        field='sales_14d',
    ).metric(
        'dsp_spend',
        'sum',
        field='total_cost',
    ).metric(
        'dsp_total_ntb_sales',
        'sum',
        field='total_new_to_brand_product_sales_14d',
    ).metric(
        'dsp_total_product_sales',
        'sum',
        field='total_sales_14d',
    ).metric(
        'dsp_total_units_sold',
        'sum',
        field='total_units_sold_14d',
    ).metric(
        'dsp_units_sold',
        'sum',
        script={
            "source": """
                if (doc.type.value == 'campaign') return doc.units_sold_14d.value;
            """
        },
    ).metric(
        'sa_attributed_conversions_14d',
        'sum',
        field='attributed_conversions_14d',
    ).metric(
        'sa_clicks',
        'sum',
        field='clicks',
    ).metric(
        'sa_dpv',
        'sum',
        field='attributed_dpv_14d',
    ).metric(
        'sa_impressions',
        'sum',
        script={
            'source': """
                if (doc.type.value == 'campaigns') return doc.impressions.value;
            """,
        },
    ).metric(
        'sa_sales',
        'sum',
        field='attributed_sales_14d_same_SKU',
    ).metric(
        'sa_spend',
        'sum',
        field='cost',
    ).metric(
        'sa_total_ntb_sales',
        'sum',
        field='attributed_sales_new_to_brand_14d',
    ).metric(
        'sa_total_product_sales',
        'sum',
        field='attributed_sales_14d',
    ).metric(
        'sa_total_units_sold',
        'sum',
        field='attributed_units_sold_14d',
    ).metric(
        'sa_units_sold',
        'sum',
        script={
            'source': """
                if (doc.type.value == 'campaigns') return doc.units_sold_14d.value;
            """,
        },
    ).metric(
        'total_impressions',
        'sum',
        field='impressions',
    ).metric(
        'cpa',
        'bucket_script',
        buckets_path={
            'sa_spend': 'sa_spend',
            'dsp_spend': 'dsp_spend',
            'sa_conversions': 'sa_attributed_conversions_14d',
            'dsp_purchases': 'dsp_purchases_14d',
        },
        script='((params.dsp_spend + params.sa_spend) / (params.dsp_purchases + params.sa_conversions))',
        gap_policy='insert_zeros',
    ).metric(
        'ctr',
        'bucket_script',
        buckets_path={
            'sa_clicks': 'sa_spend',
            'dsp_clicks': 'dsp_spend',
            'impressions': 'total_impressions',
        },
        script='((params.dsp_clicks + params.sa_clicks) / params.impressions) * 100',
        gap_policy='insert_zeros',
    ).metric(
        'dpvr',
        'bucket_script',
        buckets_path={
            'dsp_dpv': 'dsp_dpv',
            'sa_dpv': 'sa_dpv',
            'impressions': 'total_impressions',
        },
        script='((params.dsp_dpv + params.sa_dpv) / params.impressions) * 100',
        gap_policy='insert_zeros',
    ).metric(
        'ecpm',
        'bucket_script',
        buckets_path={
            'dsp_spend': 'dsp_spend',
            'dsp_ecpm': 'dsp_dpv',
            'dsp_impressions': 'dsp_impressions',
            'sa_spend': 'sa_spend',
            'sa_impressions': 'sa_impressions',
        },
        script='(((params.dsp_spend + params.sa_spend) / (params.dsp_impressions + params.sa_impressions)) * 1000)',
        gap_policy='insert_zeros',
    ).metric(
        'ntb_percentage',
        'bucket_script',
        buckets_path={
            'sa_ntb_sales': 'sa_total_ntb_sales',
            'dsp_ntb_sales': 'dsp_total_ntb_sales',
            'sa_sales': 'sa_sales',
            'dsp_sales': 'dsp_sales',
        },
        script='((params.sa_ntb_sales + params.dsp_ntb_sales) / (params.sa_sales + params.dsp_sales) * 100)',
        gap_policy='insert_zeros',
    ).metric(
        'roas',
        'bucket_script',
        buckets_path={
            'dsp_spend': 'dsp_spend',
            'dsp_sales': 'dsp_sales',
            'sa_spend': 'sa_spend',
            'sa_sales': 'sa_sales',
        },
        script='(params.dsp_sales + params.sa_sales) / (params.dsp_spend + params.sa_spend)',
        gap_policy='insert_zeros',
    ).metric(
        'total_ntb_percentage',
        'bucket_script',
        buckets_path={
            'sa_ntb_sales': 'sa_total_ntb_sales',
            'dsp_ntb_sales': 'dsp_total_ntb_sales',
            'sa_total_sales': 'sa_total_product_sales',
            'dsp_total_sales': 'dsp_total_product_sales',
        },
        script='((params.sa_ntb_sales + params.dsp_ntb_sales) / (params.sa_total_sales + params.dsp_total_sales) * 100)',
        gap_policy='insert_zeros',
    ).metric(
        'total_roas',
        'bucket_script',
        buckets_path={
            'dsp_spend': 'dsp_spend',
            'dsp_sales': 'dsp_total_product_sales',
            'sa_spend': 'sa_spend',
            'sa_sales': 'sa_total_product_sales',
        },
        script='(params.dsp_sales + params.sa_sales) / (params.dsp_spend + params.sa_spend)',
        gap_policy='insert_zeros',
    )

    return search.execute()


def dsp_dashboard_time_series(client, index, order_ids, start_date, end_date, segments, objectives, interval):
    search = Search(
        using=client,
        index=index,
    )

    must_filter = [
        Q(
            'range',
            report_date={
                'gte': start_date,
                'lte': end_date,
            },
        ),
        Q(
            'term',
            type={
                'value': 'campaign',
            },
        ),
        Q(
            'term',
            dimension={
                'value': 'order',
            },
        ),
    ]

    if order_ids:
        must_filter.append(
            Q(
                'terms',
                order_id=order_ids,
                size=Constants.ES_FILTER_ARRAY_LIMIT,
            )
        )

    query = Q(
        'bool',
        must=must_filter,
    )

    search = search.query(query)
    
    search = search.filter(
        'script',
        script={
            "source": """
                String name = doc['order_name'].value;
                
                int indexOfFunnel = name.indexOf('[F]');
                int indexOfObjective = name.indexOf('[O]');
                
                if (indexOfObjective != -1 || indexOfFunnel != -1) {
                  String objective = name.substring(indexOfObjective + 4, indexOfObjective + 6);
                  String funnel = name.substring(indexOfFunnel + 4, indexOfFunnel + 6);
                  return params.objectives.contains(objective) || params.funnels.contains(funnel);
                }
            """,
            'params':{
                'funnels': segments,
                'objectives': objectives,
            },
        },
    )

    search.aggs.bucket(
        'interval',
        'date_histogram',
        extended_bounds={
            'min': start_date,
            'max': end_date,
        },
        field='report_date',
        interval=interval,
        min_doc_count=0,
    )
    search.aggs['interval'].metric(
        'acos',
        'bucket_script',
        buckets_path={
            'cost': 'spend',
            'sales': 'sales',
        },
        script={
            'source': 'params.cost/params.sales',
        },
        gap_policy='insert_zeros',
    ).metric(
        'clicks',
        'sum',
        field='click_throughs',
    ).metric(
        'conversions',
        'sum',
        field='purchases_14d',
    ).metric(
        'cpa',
        'bucket_script',
        buckets_path={
            'cost': 'spend',
            'conversions': 'conversions',
        },
        script={
            'source': 'params.cost/params.conversions',
        },
        gap_policy='insert_zeros',
    ).metric(
        'ctr',
        'bucket_script',
        buckets_path={
            'clicks': 'clicks',
            'impressions': 'impressions',
        },
        script={
            'source': '(params.clicks/params.impressions) * 100',
        },
        gap_policy='insert_zeros',
    ).metric(
        'cvr',
        'bucket_script',
        buckets_path={
            'clicks': 'clicks',
            'conversions': 'conversions',
        },
        script={
            'source': '(params.clicks/params.conversions) * 100',
        },
        gap_policy='insert_zeros',
    ).metric(
        'dpv',
        'sum',
        field='dvp_14d',
    ).metric(
        'dpvr',
        'bucket_script',
        buckets_path={
            'dpv': 'dpv',
            'impressions': 'impressions',
        },
        script={
            'source': '(params.dpv/params.impressions) * 100',
        },
        gap_policy='insert_zeros',
    ).metric(
        'impressions',
        'sum',
        field='impressions',
    ).metric(
        'roas',
        'bucket_script',
        buckets_path={
            'sales': 'sales',
            'cost': 'spend',
        },
        script={
            'source': 'params.sales/params.cost',
        },
        gap_policy='insert_zeros',
    ).metric(
        'sales',
        'sum',
        field='sales_14d',
    ).metric(
        'spend',
        'sum',
        field='total_cost',
    ).metric(
        'total_acos',
        'bucket_script',
        buckets_path={
            'cost': 'spend',
            'sales': 'total_sales',
        },
        script={
            'source': 'params.cost/params.sales',
        },
        gap_policy='insert_zeros',
    ).metric(
        'total_roas',
        'bucket_script',
        buckets_path={
            'sales': 'total_sales',
            'cost': 'spend',
        },
        script={
            'source': 'params.sales/params.cost',
        },
        gap_policy='insert_zeros',
    ).metric(
        'total_sales',
        'sum',
        field='total_sales_14d',
    ).metric(
        'total_units_sold',
        'sum',
        field='total_units_sold_14d',
    ).metric(
        'units_sold',
        'sum',
        field='units_sold_14d',
    )

    search.aggs.bucket(
        'histogram_for_average',
        'date_histogram',
        field='report_date',
        interval='day',
    )
    search.aggs['histogram_for_average'].metric(
        'sales',
        'sum',
        field='sales_14d',
    ).metric(
        'spend',
        'sum',
        field='total_cost',
    ).metric(
        'roas',
        'bucket_script',
        buckets_path={
            'sales': 'sales',
            'cost': 'spend',
        },
        script={
            'source': 'params.sales/params.cost',
        },
        gap_policy='insert_zeros',
    ).metric(
        'total_roas',
        'bucket_script',
        buckets_path={
            'sales': 'total_sales',
            'cost': 'spend',
        },
        script={
            'source': 'params.sales/params.cost',
        },
        gap_policy='insert_zeros',
    ).metric(
        'total_sales',
        'sum',
        field='total_sales_14d',
    )

    search.aggs.metric(
        'average_sales',
        'avg_bucket',
        buckets_path='histogram_for_average>sales',
    ).metric(
        'average_spend',
        'avg_bucket',
        buckets_path='histogram_for_average>spend',
    ).metric(
        'average_roas',
        'avg_bucket',
        buckets_path='histogram_for_average>roas',
    ).metric(
        'average_total_roas',
        'avg_bucket',
        buckets_path='histogram_for_average>total_roas',
    ).metric(
        'average_total_sales',
        'avg_bucket',
        buckets_path='histogram_for_average>total_sales',
    )
    
    return search.execute()


def dsp_model_aggregation(client, index, model, model_ids, start_date, end_date):
    search = Search(
        using=client,
        index=index,
    )

    model_id = f'{model}_id'
    keys = {
        model_id: model_ids,
    }

    must = [
        Q(
            'bool',
            minimum_should_match=1,
            should=[
                Q(
                    'term',
                    segment={
                        'value': 'null',
                    },
                ),
                Q(
                    'bool',
                    must_not=[
                        Q(
                            'exists',
                            field='segment',
                        ),
                    ],
                ),
            ],
        ),
        Q(
            'term',
            dimension={
                'value': data_utility.to_camel_case(model),
            }
        ),
        Q(
            'term',
            type={
                'value': 'campaign',
            },
        ),
        Q(
            'terms',
            **keys,
        ),
    ]

    if start_date and end_date:
        must.append(
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        )

    search = search.query(
        Q(
            'bool',
            must=must,
        ),
    )

    model_aggregation = A(
        'terms',
        field=model_id,
        size=Constants.ES_FILTER_ARRAY_LIMIT,
    ).metric(
        'total_clicks',
        'sum',
        field='click_throughs',
    ).metric(
        'total_impressions',
        'sum',
        field='impressions',
    ).metric(
        'total_sales',
        'sum',
        field='sales_14d',
    ).metric(
        'total_spend',
        'sum',
        field='total_cost',
    ).metric(
        'ctr',
        'bucket_script',
        buckets_path={
            'clicks': 'total_clicks',
            'impressions': 'total_impressions',
        },
        script={
            'source': '(params.clicks/params.impressions) * 100',
        },
        gap_policy='insert_zeros',
    ).metric(
        'total_attributed_sales',
        'sum',
        field='total_sales_14d',
    ).metric(
        'total_units_sold',
        'sum',
        field='total_units_sold_14d',
    ).metric(
        'units_sold',
        'sum',
        field='units_sold_14d',
    )


    search.aggs.bucket(
        'results',
        model_aggregation,
    )

    return search.execute()


def dsp_objectives_time_series(client, index, start_date, end_date, interval, objectives, segments):
    search = Search(
        using=client,
        index=index,
    )

    query = Q(
        'bool',
        must=[
            Q(
                'term',
                type={
                    'value': 'campaign',
                },
            ),
            Q(
                'term',
                dimension={
                    'value': 'order',
                },
            ),
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        ],
    )

    search = search.query(query)

    if objectives or segments:
        search = search.filter(
            'script',
            script={
                "source": """
                    String name = doc['order_name'].value;
                    
                    int indexOfFunnel = name.indexOf('[F]');
                    int indexOfObjective = name.indexOf('[O]');
                    
                    if (indexOfObjective != -1 || indexOfFunnel != -1) {
                        String objective = name.substring(indexOfObjective + 4, indexOfObjective + 6);
                        String funnel = name.substring(indexOfFunnel + 4, indexOfFunnel + 6);
                        return params.objectives.contains(objective) || params.funnels.contains(funnel);
                    }
                """,
                'params': {
                    'funnels': segments,
                    'objectives': objectives,
                },
            },
        )

    search.aggs.bucket(
        'interval',
        'date_histogram',
        extended_bounds={
            'min': start_date,
            'max': end_date,
        },
        field='report_date',
        interval=interval.value,
        min_doc_count=0,
    )
    search.aggs['interval'].metric(
        'acos',
        'bucket_script',
        buckets_path={
            'cost': 'spend',
            'sales': 'sales',
        },
        script='params.cost/params.sales',
        gap_policy='insert_zeros',
    ).metric(
        'acpc',
        'bucket_script',
        buckets_path={
            'clicks': 'clicks',
            'spend': 'spend',
        },
        script='params.spend/params.clicks',
    ).metric(
        'clicks',
        'sum',
        field='click_throughs',
    ).metric(
        'conversions',
        'sum',
        field='purchases_14d',
    ).metric(
        'cpa',
        'bucket_script',
        buckets_path={
            'cost': 'spend',
            'purchases': 'purchases',
        },
        script='params.cost/params.purchases',
    ).metric(
        'ctr',
        'bucket_script',
        buckets_path={
            'clicks': 'clicks',
            'impressions': 'impressions',
        },
        script='(params.clicks/params.impressions) * 100',
    ).metric(
        'cvr',
        'bucket_script',
        buckets_path={
            'clicks': 'clicks',
            'conversions': 'conversions',
        },
        script='(params.conversions/params.clicks) * 100',
        gap_policy='insert_zeros',
    ).metric(
        'dpv',
        'sum',
        field='dpv_14d',
    ).metric(
        'dpvr',
        'avg',
        field='dprv_14d',
    ).metric(
        'ecpm',
        'avg',
        field='e_cpm',
    ).metric(
        'impressions',
        'sum',
        field='impressions',
    ).metric(
        'ntb_percentage',
        'bucket_script',
        buckets_path={
            'ntb_sales': 'total_ntb_sales',
            'sales': 'sales',
        },
        script='((params.ntb_sales / params.sales) * 100)',
        gap_policy='insert_zeros',
    ).metric(
        'purchases',
        'sum',
        field='purchases_14d',
    ).metric(
        'roas',
        'bucket_script',
        buckets_path={
            'sales': 'sales',
            'spend': 'spend',
        },
        script='params.sales/params.spend',
        gap_policy='insert_zeros',
    ).metric(
        'sales',
        'sum',
        field='sales_14d',
    ).metric(
        'spend',
        'sum',
        field='total_cost',
    ).metric(
        'total_ntb_sales',
        'sum',
        field='total_new_to_brand_product_sales_14d',
    ).metric(
        'total_product_sales',
        'sum',
        field='total_sales_14d',
    ).metric(
        'total_ntb_percentage',
        'bucket_script',
        buckets_path={
            'ntb_sales': 'total_ntb_sales',
            'total_sales': 'total_product_sales',
        },
        script='((params.ntb_sales / params.total_sales) * 100)',
        gap_policy='insert_zeros',
    ).metric(
        'total_roas',
        'bucket_script',
        buckets_path={
            'sales': 'total_product_sales',
            'spend': 'spend',
        },
        script='params.sales/params.spend',
        gap_policy='insert_zeros',
    ).metric(
        'total_units_sold',
        'sum',
        field='total_units_sold_14d',
    ).metric(
        'units_sold',
        'sum',
        field='units_sold_14d',
    )

    return search.execute()


def engagement_time_series(client, index, start_date, end_date, interval):
    search = Search(
        using=client,
        index=index,
    )

    query = Q(
        'bool',
        minimum_should_match=1,
        should=[
            Q(
                'term',
                type={
                    'value': 'campaign',
                },
            ),
            Q(
                'term',
                type={
                    'value': 'campaigns',
                },
            ),
        ],
        must=[
            Q(
                'bool',
                minimum_should_match=1,
                should=[
                    Q(
                        'term',
                        segment={
                            'value': 'null',
                        },
                    ),
                    Q(
                        'bool',
                        minimum_should_match=1,
                        should=[
                            Q(
                                'bool',
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                    Q(
                                        'exists',
                                        field='dimension',
                                    ),
                                ],
                            ),
                            Q(
                                'bool',
                                must=[
                                    Q(
                                        'term',
                                        dimension={
                                            'value': 'order',
                                        },
                                    ),
                                ],
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                ]
                            )
                        ]
                    )
                ]
            ),
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        ],
    )

    search = search.query(query)

    search.aggs.bucket(
        'interval',
        'date_histogram',
        extended_bounds={
            'min': start_date,
            'max': end_date,
        },
        field='report_date',
        interval=interval,
        min_doc_count=0,
    )
    search.aggs['interval'].metric(
        'dsp_clicks',
        'sum',
        field='click_throughs',
    ).metric(
        'dsp_impressions',
        'sum',
        script={
            'source': """
                if (doc.type.value == 'campaign') return doc.impressions.value;
            """
        },
    ).metric(
        'sa_clicks',
        'sum',
        field='clicks',
    ).metric(
        'sa_impressions',
        'sum',
        script={
            'source': """
                if (doc.type.value == 'campaigns') return doc.impressions.value;
            """
        },
    ).metric(
        'ctr',
        'bucket_script',
        buckets_path={
            'dsp_clicks': 'dsp_clicks',
            'dsp_impressions': 'dsp_impressions',
            'sa_clicks': 'sa_clicks',
            'sa_impressions': 'sa_impressions',
        },
        script='((params.dsp_clicks + params.sa_clicks)/(params.dsp_impressions + params.sa_impressions)) * 100',
    )
    
    return search.execute()


def my_dashboard_statistics(client, index, start_date, end_date, interval, table):
    search = Search(
        using=client,
        index=index,
    )

    if table == TableType.ALL:
        query = Q(
            'bool',
            minimum_should_match=1,
            must=[
                Q(
                    'bool',
                    minimum_should_match=1,
                    should=[
                        Q(
                            'term',
                            segment={
                                'value': 'null',
                            },
                        ),
                        Q(
                            'bool',
                            minimum_should_match=1,
                            should=[
                                Q(
                                    'bool',
                                    must_not=[
                                        Q(
                                            'exists',
                                            field='dimension',
                                        ),
                                        Q(
                                            'exists',
                                            field='segment',
                                        ),
                                    ],
                                ),
                                Q(
                                    'bool',
                                    must=[
                                        Q(
                                            'term',
                                            dimension={
                                                'value': 'order',
                                            },
                                        ),
                                    ],
                                    must_not=[
                                        Q(
                                            'exists',
                                            field='segment',
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
                Q(
                    'range',
                    report_date={
                        'gte': start_date,
                        'lte': end_date,
                    }
                )
            ],
            should=[
                Q(
                    'term',
                    type={
                        'value': 'campaign',
                    },
                ),
                Q(
                    'term',
                    type={
                        'value': 'campaigns',
                    },
                ),
            ],
        )

        search = search.query(query)

        search.aggs.bucket(
            'interval',
            'date_histogram',
            extended_bounds={
                'min': start_date,
                'max': end_date,
            },
            field='report_date',
            interval=interval,
            min_doc_count=0,
        )
        search.aggs['interval'].metric(
            'dsp_clicks',
            'sum',
            field='click_throughs',
        ).metric(
            'dsp_impressions',
            'sum',
            script={
                "source": """
                    if (doc.type.value == 'campaign') return doc.impressions.value;
                """
            },
        ).metric(
            'dsp_sales',
            'sum',
            field='sales_14d',
        ).metric(
            'dsp_spend',
            'sum',
            field='total_cost',
        ).metric(
            'dsp_total_sales',
            'sum',
            field='total_sales_14d',
        ).metric(
            'sa_clicks',
            'sum',
            field='clicks',
        ).metric(
            'sa_impressions',
            'sum',
            script={
                "source": """
                    if (doc.type.value == 'campaigns') return doc.impressions.value;
                """
            },
        ).metric(
            'sa_sales',
            'sum',
            field='attributed_sales_14d_same_SKU',
        ).metric(
            'sa_spend',
            'sum',
            field='cost',
        ).metric(
            'sa_total_sales',
            'sum',
            field='attributed_sales_14d',
        ).metric(
            'acpc',
            'bucket_script',
            buckets_path={
                'dsp_clicks': 'dsp_clicks',
                'dsp_cost': 'dsp_spend',
                'sa_clicks': 'sa_clicks',
                'sa_cost': 'sa_spend',
            },
            script='((params.sa_cost+params.dsp_cost)/(params.sa_clicks+params.dsp_clicks))',
        ).metric(
            'ctr',
            'bucket_script',
            buckets_path={
                'dsp_clicks': 'dsp_clicks',
                'dsp_impressions': 'dsp_impressions',
                'sa_clicks': 'sa_clicks',
                'sa_impressions': 'sa_impressions',
            },
            script='((params.sa_clicks+params.dsp_clicks)/(params.sa_impressions+params.dsp_impressions)) * 100',
            gap_policy='insert_zeros',
        ).metric(
            'roas',
            'bucket_script',
            buckets_path={
                'dsp_sales': 'dsp_sales',
                'dsp_spend': 'sa_spend',
                'sa_sales': 'sa_sales',
                'sa_spend': 'sa_spend',
            },
            script='((params.sa_sales + params.dsp_sales)/(params.dsp_spend + params.sa_spend))',
            gap_policy='insert_zeros',
        ).metric(
            'total_roas',
            'bucket_script',
            buckets_path={
                'dsp_sales': 'dsp_total_sales',
                'dsp_spend': 'sa_spend',
                'sa_sales': 'sa_total_sales',
                'sa_spend': 'sa_spend',
            },
            script='((params.sa_sales + params.dsp_sales)/(params.dsp_spend + params.sa_spend))',
            gap_policy='insert_zeros',
        )
    else:
        query = Q(
            'bool',
            minimum_should_match=1,
            should=[
                Q(
                    'term',
                    type={
                        'value': 'campaign',
                    },
                ),
                Q(
                    'term',
                    type={
                        'value': 'campaigns',
                    },
                ),
            ],
            must=[
                Q(
                    'bool',
                    minimum_should_match=1,
                    should=[
                        Q(
                            'term',
                            segment={
                                'value': 'null',
                            },
                        ),
                        Q(
                            'bool',
                            minimum_should_match=1,
                            should=[
                                Q(
                                    'bool',
                                    must_not=[
                                        Q(
                                            'exists',
                                            field='segment',
                                        ),
                                        Q(
                                            'exists',
                                            field='dimension',
                                        ),
                                    ],
                                ),
                                Q(
                                    'bool',
                                    must=[
                                        Q(
                                            'term',
                                            dimension={
                                                'value': 'order',
                                            },
                                        ),
                                    ],
                                    must_not=[
                                        Q(
                                            'exists',
                                            field='segment',
                                        ),
                                    ]
                                )
                            ],
                        ),
                    ],
                ),
                Q(
                    'range',
                    report_date={
                        'gte': start_date,
                        'lte': end_date,
                    },
                ),
            ]
        )

        search = search.query(query)

        search.aggs.bucket(
            'interval',
            'date_histogram',
            extended_bounds={
                'min': start_date,
                'max': end_date,
            },
            field='report_date',
            interval=interval,
            min_doc_count=0,
        )

        ad_type_aggregation = A(
            'terms',
            script={
                "source": """
                    String type = doc.type.value;
                    if( type == "campaigns" ) {
                      return doc['ad_type'];
                    } else {
                      return 'dsp';
                    }
                """
            }
        ).metric(
            'sales',
            'sum',
            script={
                "source": """
                    String type = doc.type.value;
                    if( type == "campaigns" ) {
                      return doc['attributed_sales_14d_same_SKU'];
                    } else {
                      return doc['sales_14d'];
                    }
                """
            },
        ).metric(
            'spend',
            'sum',
            script={
                "source": """
                    String type = doc.type.value;
                    if( type == "campaigns" ) {
                      return doc['cost'];
                    } else {
                      return doc['total_cost'];
                    }
                """
            },
        ).metric(
            'total_sales',
            'sum',
            script={
                "source": """
                    String type = doc.type.value;
                    if( type == "campaigns" ) {
                      return doc['attributed_sales_14d'];
                    } else {
                      return doc['total_sales_14d'];
                    }
                """
            },
        )

        search.aggs['interval'].bucket(
            'ad_type',
            ad_type_aggregation,
        )

    return search.execute()


def portfolio_aggregation(client, index, model_ids, start_date, end_date):
    search = Search(
        using=client,
        index=index,
    )

    must = [
        Q(
            'bool',
            minimum_should_match=1,
            should=[
                Q(
                    'term',
                    segment={
                        'value': 'null',
                    },
                ),
                Q(
                    'bool',
                    must_not=[
                        Q(
                            'exists',
                            field='segment',
                        ),
                    ],
                ),
            ],
        ),
        Q(
            'term',
            type={
                'value': 'campaigns',
            },
        ),
        Q(
            'terms',
            campaign_id=model_ids,
        ),
    ]

    # TODO: Are start_date, end_date optional?
    if start_date and end_date:
        must.append(
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        )

    search = search.query(
        Q(
            'bool',
            must=must,
        ),
    )

    model_aggregation = A(
        'terms',
        size=Constants.ES_FILTER_ARRAY_LIMIT,
        field='type',
    ).metric(
        'total_clicks',
        'sum',
        field='clicks',
    ).metric(
        'total_impressions',
        'sum',
        field='impressions',
    ).metric(
        'total_sales',
        'sum',
        field='attributed_sales_14d',
    ).metric(
        'total_spend',
        'sum',
        field='cost',
    ).metric(
        'ctr',
        'bucket_script',
        buckets_path={
            'clicks': 'total_clicks',
            'impressions': 'total_impressions',
        },
        script={
            'source': '(params.clicks/params.impressions) * 100',
        },
        gap_policy='insert_zeros',
    ).metric(
        'total_units_sold',
        'sum',
        field='attributed_units_sold_14d',
    ).metric(
        'units_sold',
        'sum',
        field='units_sold_14d',
    )

    search.aggs.bucket(
        'results',
        model_aggregation,
    )

    return search.execute()


def portfolios_dashboard_time_series(client, index, start_date, end_date, campaign_ids, interval, objectives):
    search = Search(
        using=client,
        index=index,
    )

    query = Q(
        'bool',
        must=[
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
            Q(
                'term',
                type={
                    'value': 'campaigns',
                },
            ),
            Q(
                'terms',
                campaign_id=campaign_ids,
            ),
            Q(
                'bool',
                minimum_should_match=1,
                should=[
                    Q(
                        'term',
                        segment={
                            'value': 'null',
                        },
                    ),
                    Q(
                        'bool',
                        must_not=[
                            Q(
                                'exists',
                                field='segment',
                            ),
                        ],
                    ),
                ]
            )
        ],
    )

    search = search.query(query)
    
    search = search.filter(
        'script',
        script={
            "source": """
                String campaignName = doc['campaign_name'].value;

                if (campaignName.length() < 2) { return false; }

                String objective = campaignName.substring(0,2);
                return params.objectives.contains(objective);
            """,
            'params': {
                'objectives': objectives,
            },
        },
    )

    search.aggs.bucket(
        'interval',
        'date_histogram',
        extended_bounds={
            'min': start_date,
            'max': end_date,
        },
        field='report_date',
        interval=interval,
        min_doc_count=0,
    )
    search.aggs['interval'].metric(
        'acos',
        'bucket_script',
        buckets_path={
            'cost': 'spend',
            'sales': 'sales',
        },
        script={
            'source': 'params.cost/params.sales',
        },
        gap_policy='insert_zeros',
    ).metric(
        'clicks',
        'sum',
        field='clicks',
    ).metric(
        'ctr',
        'bucket_script',
        buckets_path={
            'clicks': 'clicks',
            'impressions': 'impressions',
        },
        script={
            'source': '(params.clicks/params.impressions) * 100',
        },
        gap_policy='insert_zeros',
    ).metric(
        'impressions',
        'sum',
        field='impressions',
    ).metric(
        'roas',
        'bucket_script',
        buckets_path={
            'sales': 'sales',
            'cost': 'spend',
        },
        script={
            'source': 'params.sales/params.cost',
        },
        gap_policy='insert_zeros',
    ).metric(
        'sales',
        'sum',
        field='attributed_sales_14d_same_SKU',
    ).metric(
        'spend',
        'sum',
        field='cost',
    ).metric(
        'total_acos',
        'bucket_script',
        buckets_path={
            'cost': 'spend',
            'sales': 'total_sales',
        },
        script={
            'source': 'params.cost/params.sales',
        },
        gap_policy='insert_zeros',
    ).metric(
        'total_roas',
        'bucket_script',
        buckets_path={
            'sales': 'total_sales',
            'cost': 'spend',
        },
        script={
            'source': 'params.sales/params.cost',
        },
        gap_policy='insert_zeros',
    ).metric(
        'total_sales',
        'sum',
        field='attributed_sales_14d',
    ).metric(
        'total_units_sold',
        'sum',
        field='attributed_units_sold_14d',
    ).metric(
        'units_sold',
        'sum',
        field='units_sold',
    )

    search.aggs.bucket(
        'histogram_for_average',
        'date_histogram',
        field='report_date',
        interval='day',
    )
    search.aggs['histogram_for_average'].metric(
        'sales',
        'sum',
        field='attributed_sales_14d_same_SKU',
    ).metric(
        'spend',
        'sum',
        field='cost',
    ).metric(
        'roas',
        'bucket_script',
        buckets_path={
            'sales': 'sales',
            'cost': 'spend',
        },
        script={
            'source': 'params.sales/params.cost',
        },
        gap_policy='insert_zeros',
    ).metric(
        'total_roas',
        'bucket_script',
        buckets_path={
            'sales': 'total_sales',
            'cost': 'spend',
        },
        script={
            'source': 'params.sales/params.cost',
        },
        gap_policy='insert_zeros',
    ).metric(
        'total_sales',
        'sum',
        field='attributed_sales_14d',
    )

    search.aggs.metric(
        'average_sales',
        'avg_bucket',
        buckets_path='histogram_for_average>sales',
    ).metric(
        'average_spend',
        'avg_bucket',
        buckets_path='histogram_for_average>spend',
    ).metric(
        'average_roas',
        'avg_bucket',
        buckets_path='histogram_for_average>roas',
    ).metric(
        'average_total_roas',
        'avg_bucket',
        buckets_path='histogram_for_average>total_roas',
    ).metric(
        'average_total_sales',
        'avg_bucket',
        buckets_path='histogram_for_average>total_sales',
    )
    
    return search.execute()


def sa_and_dsp_sales_vs_roas_time_series(client, index, start_date, end_date, interval):
    search = Search(
        using=client,
        index=index,
    )

    query = Q(
        'bool',
        minimum_should_match=1,
        should=[
            Q(
                'term',
                type={
                    'value': 'campaign',
                },
            ),
            Q(
                'term',
                type={
                    'value': 'campaigns',
                },
            ),
        ],
        must=[
            Q(
                'bool',
                minimum_should_match=1,
                should=[
                    Q(
                        'term',
                        segment={
                            'value': 'null',
                        },
                    ),
                    Q(
                        'bool',
                        minimum_should_match=1,
                        should=[
                            Q(
                                'bool',
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                    Q(
                                        'exists',
                                        field='dimension',
                                    ),
                                ],
                            ),
                            Q(
                                'bool',
                                must=[
                                    Q(
                                        'term',
                                        dimension={
                                            'value': 'order',
                                        },
                                    ),
                                ],
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                ],
                            ),
                        ],
                    ),
                ]
            ),
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        ]
    )

    search = search.query(query)

    search.aggs.bucket(
        'interval',
        'date_histogram',
        extended_bounds={
            'min': start_date,
            'max': end_date,
        },
        field='report_date',
        interval=interval,
        min_doc_count=0,
    )
    
    search.aggs['interval'].metric(
        'dsp_sales',
        'sum',
        field='sales_14d',
    ).metric(
        'dsp_spend',
        'sum',
        field='total_cost',
    ).metric(
        'dsp_total_sales',
        'sum',
        field='total_sales_14d',
    ).metric(
        'sa_sales',
        'sum',
        field='attributed_sales_14d_same_SKU',
    ).metric(
        'sa_spend',
        'sum',
        field='cost',
    ).metric(
        'sa_total_sales',
        'sum',
        field='attributed_sales_14d',
    ).metric(
        'roas',
        'bucket_script',
        buckets_path={
            'dsp_sales': 'dsp_sales',
            'dsp_spend': 'dsp_spend',
            'sa_sales': 'sa_sales',
            'sa_spend': 'sa_spend',
        },
        script='((params.dsp_sales + params.sa_sales)/(params.dsp_spend + params.sa_spend))',
        gap_policy='insert_zeros',
    ).metric(
        'total_roas',
        'bucket_script',
        buckets_path={
            'dsp_sales': 'dsp_total_sales',
            'dsp_spend': 'dsp_spend',
            'sa_sales': 'sa_total_sales',
            'sa_spend': 'sa_spend',
        },
        script='((params.dsp_sales + params.sa_sales)/(params.dsp_spend + params.sa_spend))',
        gap_policy='insert_zeros',
    )

    return search.execute()


def sa_dashboard_time_series(client, index, campaign_ids, start_date, end_date, objectives, ad_type, interval):
    search = Search(
        using=client,
        index=index,
    )

    must_filter = [
        Q(
            'range',
            report_date={
                'gte': start_date,
                'lte': end_date,
            },
        ),
        Q(
            'term',
            type={
                'value': 'campaigns',
            },
        ),
        Q(
            'term',
            ad_type={
                'value': ad_type,
            },
        ),
        Q(
            'bool',
            minimum_should_match=1,
            should=[
                Q(
                    'term',
                    segment={
                        'value': 'null',
                    },
                ),
                Q(
                    'bool',
                    must_not=[
                        Q(
                            'exists',
                            field='segment',
                        ),
                    ],
                ),
            ]
        )
    ]

    if campaign_ids:
        must_filter.append(
            Q(
                'terms',
                campaign_id=campaign_ids,
                size=Constants.ES_FILTER_ARRAY_LIMIT,
            )
        )

    query = Q(
        'bool',
        must=must_filter,
    )

    search = search.query(query)
    
    search = search.filter(
        'script',
        script={
            "source": """
                String campaignName = doc['campaign_name'].value;

                if (campaignName.length() < 2) { return false; }

                String objective = campaignName.substring(0,2);
                return params.objectives.contains(objective);
            """,
            'params': {
                'objectives': objectives,
            },    
        },
    )

    search.aggs.bucket(
        'interval',
        'date_histogram',
        extended_bounds={
            'min': start_date,
            'max': end_date,
        },
        field='report_date',
        interval=interval,
        min_doc_count=0,
    )
    search.aggs['interval'].metric(
        'acos',
        'bucket_script',
        buckets_path={
            'cost': 'spend',
            'sales': 'sales',
        },
        script={
            'source': 'params.cost/params.sales',
        },
        gap_policy='insert_zeros',
    ).metric(
        'clicks',
        'sum',
        field='clicks',
    ).metric(
        'ctr',
        'bucket_script',
        buckets_path={
            'clicks': 'clicks',
            'impressions': 'impressions',
        },
        script={
            'source': '(params.clicks/params.impressions) * 100',
        },
        gap_policy='insert_zeros',
    ).metric(
        'impressions',
        'sum',
        field='impressions',
    ).metric(
        'roas',
        'bucket_script',
        buckets_path={
            'sales': 'sales',
            'cost': 'spend',
        },
        script={
            'source': 'params.sales/params.cost',
        },
        gap_policy='insert_zeros',
    ).metric(
        'sales',
        'sum',
        field='attributed_sales_14d_same_SKU',
    ).metric(
        'spend',
        'sum',
        field='cost',
    ).metric(
        'total_acos',
        'bucket_script',
        buckets_path={
            'cost': 'spend',
            'sales': 'total_sales',
        },
        script={
            'source': 'params.cost/params.sales',
        },
        gap_policy='insert_zeros',
    ).metric(
        'total_roas',
        'bucket_script',
        buckets_path={
            'sales': 'total_sales',
            'cost': 'spend',
        },
        script={
            'source': 'params.sales/params.cost',
        },
        gap_policy='insert_zeros',
    ).metric(
        'total_sales',
        'sum',
        field='attributed_sales_14d',
    ).metric(
        'units_sold',
        'sum',
        field='units_sold_14d',
    )

    if ad_type in [Constants.SPONSORED_DISPLAY, Constants.SPONSORED_PRODUCTS]:
        search.aggs['interval'].metric(
            'total_units_sold',
            'sum',
            field='attributed_units_sold_14d',
        )

    search.aggs.bucket(
        'histogram_for_average',
        'date_histogram',
        field='report_date',
        interval='day',
    )
    search.aggs['histogram_for_average'].metric(
        'sales',
        'sum',
        field='attributed_sales_14d_same_SKU',
    ).metric(
        'spend',
        'sum',
        field='cost',
    ).metric(
        'roas',
        'bucket_script',
        buckets_path={
            'sales': 'sales',
            'cost': 'spend',
        },
        script={
            'source': 'params.sales/params.cost',
        },
        gap_policy='insert_zeros',
    ).metric(
        'total_roas',
        'bucket_script',
        buckets_path={
            'sales': 'total_sales',
            'cost': 'spend',
        },
        script={
            'source': 'params.sales/params.cost',
        },
        gap_policy='insert_zeros',
    ).metric(
        'total_sales',
        'sum',
        field='attributed_sales_14d',
    )

    search.aggs.metric(
        'average_sales',
        'avg_bucket',
        buckets_path='histogram_for_average>sales',
    ).metric(
        'average_spend',
        'avg_bucket',
        buckets_path='histogram_for_average>spend',
    ).metric(
        'average_roas',
        'avg_bucket',
        buckets_path='histogram_for_average>roas',
    ).metric(
        'average_total_roas',
        'avg_bucket',
        buckets_path='histogram_for_average>total_roas',
    ).metric(
        'average_total_sales',
        'avg_bucket',
        buckets_path='histogram_for_average>total_sales',
    )

    return search.execute()


def sa_model_aggregation(client, index, api, model, model_ids, start_date, end_date):
    search = Search(
        using=client,
        index=index,
    )

    model_id = f'{model}_id'
    if model == 'product_ad':
        model_id = 'ad_id'

    keys = {
        model_id: model_ids,
    }

    must = [
        Q(
            'bool',
            minimum_should_match=1,
            should=[
                Q(
                    'term',
                    segment={
                        'value': 'null',
                    },
                ),
                Q(
                    'bool',
                    must_not=[
                        Q(
                            'exists',
                            field='segment',
                        ),
                    ],
                ),
            ],
        ),
        Q(
            'term',
            type={
                'value': f'{data_utility.to_camel_case(model)}s',
            },
        ),
        Q(
            'terms',
            **keys,
        ),
    ]

    # POSSIBLE: Handles portfolios exception, which use campaigns
    # but do not have API
    if model == Constants.CAMPAIGN and api is not None:
        must.append(
            Q(
                'term',
                ad_type={
                    'value': api,
                }
            )
        )

    # start_date, end_date do not apply when viewing, for example,
    # a campaign's ad groups (at least, initially when first selected)
    if start_date and end_date:
        must.append(
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        )

    search = search.query(
        Q(
            'bool',
            must=must,
        ),
    )

    model_aggregation = A(
        'terms',
        field=model_id,
        size=Constants.ES_FILTER_ARRAY_LIMIT,
    ).metric(
        'total_attributed_sales',
        'sum',
        field='attributed_sales_14d',
    ).metric(
        'total_clicks',
        'sum',
        field='clicks',
    ).metric(
        'total_impressions',
        'sum',
        field='impressions',
    ).metric(
        'total_sales',
        'sum',
        field='attributed_sales_14d_same_SKU',
    ).metric(
        'total_spend',
        'sum',
        field='cost',
    ).metric(
        'ctr',
        'bucket_script',
        buckets_path={
            'clicks': 'total_clicks',
            'impressions': 'total_impressions',
        },
        script={
            'source': '(params.clicks/params.impressions) * 100',
        },
        gap_policy='insert_zeros',
    ).metric(
        'total_units_sold',
        'sum',
        field='attributed_units_sold_14d',
    ).metric(
        'units_sold',
        'sum',
        field='units_sold_14d',
    )

    search.aggs.bucket(
        'results',
        model_aggregation,
    )
    
    return search.execute()


def sa_objectives_time_series(client, index, start_date, end_date, interval, objectives):
    search = Search(
        using=client,
        index=index,
    )

    query = Q(
        'bool',
        must=[
            Q(
                'bool',
                minimum_should_match=1,
                should=[
                    Q(
                        'term',
                        segment={
                            'value': 'null',
                        },
                    ),
                    Q(
                        'bool',
                        must_not=[
                            Q(
                                'exists',
                                field='segment',
                            ),
                        ],
                    ),
                ],
            ),
            Q(
                'term',
                type={
                    'value': 'campaigns',
                },
            ),
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        ],
    )

    search.query = query

    if objectives:
        search = search.filter(
            'script',
            script={
                "source": """
                    String campaignName = doc['campaign_name'].value;

                    if (campaignName.length() < 2) { return false; }

                    String objective = campaignName.substring(0,2);
                    return params.objectives.contains(objective);
                """,
                'params': {
                    'objectives': objectives,
                },
            },
        )

    search.aggs.bucket(
        'interval',
        'date_histogram',
        extended_bounds={
            'min': start_date,
            'max': end_date,
        },
        field='report_date',
        interval=interval,
        min_doc_count=0,
    )
    search.aggs['interval'].metric(
        'clicks',
        'sum',
        field='clicks',
    ).metric(
        'acpc',
        'bucket_script',
        buckets_path={
            'clicks': 'clicks',
            'spend': 'spend',
        },
        script='params.spend/params.clicks',
    ).metric(
        'conversions',
        'sum',
        field='attributed_conversions_14d',
    ).metric(
        'cpa',
        'bucket_script',
        buckets_path={
            'conversions': 'conversions',
            'cost': 'spend',
        },
        script='params.cost/params.conversions',
    ).metric(
        'ctr',
        'bucket_script',
        buckets_path={
            'clicks': 'clicks',
            'impressions': 'impressions',
        },
        script='(params.clicks/params.impressions) * 100',
    ).metric(
        'dpv',
        'sum',
        field='attributed_dpv_14d',
    ).metric(
        'dpvr',
        'bucket_script',
        buckets_path={
            'dpv': 'dpv',
            'impressions': 'impressions',
        },
        script='params.dpv/params.impressions',
    ).metric(
        'ecpm',
        'bucket_script',
        buckets_path={
            'cost': 'spend',
            'impressions': 'impressions',
        },
        script='(params.cost/params.impressions) * 1000',
    ).metric(
        'impressions',
        'sum',
        field='impressions',
    ).metric(
        'roas',
        'bucket_script',
        buckets_path={
            'sales': 'sales',
            'spend': 'spend',
        },
        script='params.sales/params.spend',
    ).metric(
        'sales',
        'sum',
        field='attributed_sales_14d_same_SKU',
    ).metric(
        'spend',
        'sum',
        field='cost',
    ).metric(
        'total_ntb_sales',
        'sum',
        field='attributed_sales_new_to_brand_14d',
    ).metric(
        'total_product_sales',
        'sum',
        field='attributed_sales_14d',
    ).metric(
        'total_roas',
        'bucket_script',
        buckets_path={
            'sales': 'total_product_sales',
            'spend': 'spend',
        },
        script='params.sales/params.spend',
    ).metric(
        'total_units_sold',
        'sum',
        field='attributed_units_sold_14d',
    ).metric(
        'units_sold',
        'sum',
        field='units_sold_14d',
    )

    return search.execute()


def sales_and_spend_aggregation(client, index, start_date, end_date):
    search = Search(
        using=client,
        index=index,
    )

    query = Q(
        'bool',
        minimum_should_match=1,
        should=[
            Q(
                'term',
                type={
                    'value': 'campaign',
                }
            ),
            Q(
                'term',
                type={
                    'value': 'campaigns',
                }
            ),
        ],
        must=[
            Q(
                'bool',
                minimum_should_match=1,
                should=[
                    Q(
                        'term',
                        segment={
                            'value': 'null',
                        },
                    ),
                    Q(
                        'bool',
                        minimum_should_match=1,
                        should=[
                            Q(
                                'bool',
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                    Q(
                                        'exists',
                                        field='dimension',
                                    ),
                                ],
                            ),
                        ],
                    ),
                    Q(
                        'bool',
                        must=[
                            Q(
                                'term',
                                dimension={
                                    'value': 'order',
                                },
                            ),
                        ],
                        must_not=[
                            Q(
                                'exists',
                                field='segment',
                            ),
                        ],
                    ),
                ]
            ),
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        ]
    )

    search = search.query(query)

    search.aggs.metric(
        'dsp_sales',
        'sum',
        field='sales_14d',
    ).metric(
        'dsp_spend',
        'sum',
        field='total_cost',
    ).metric(
        'dsp_total_sales',
        'sum',
        field='total_sales_14d',
    ).metric(
        'sa_sales',
        'sum',
        field='attributed_sales_14d_same_SKU',
    ).metric(
        'sa_spend',
        'sum',
        field='cost',
    ).metric(
        'sa_total_sales',
        'sum',
        field='attributed_sales_14d',
    )

    return search.execute()


def sales_and_spend_by_objective_aggregation(client, index, start_date, end_date, previous_start_date, previous_end_date):
    search = Search(
        using=client,
        index=index,
    )

    query = Q(
        'bool',
        minimum_should_match=1,
        should=[
            Q(
                'term',
                type={
                    'value': 'campaign',
                },
            ),
            Q(
                'term',
                type={
                    'value': 'campaigns',
                },
            ),
        ],
        must=[
            Q(
                'bool',
                minimum_should_match=1,
                should=[
                    Q(
                        'term',
                        segment={
                            'value': 'null',
                        },
                    ),
                    Q(
                        'bool',
                        should=[
                            Q(
                                'bool',
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                    Q(
                                        'exists',
                                        field='dimension',
                                    )
                                ],
                            ),
                            Q(
                                'bool',
                                must=[
                                    Q(
                                        'term',
                                        dimension={
                                            'value': 'order',
                                        },
                                    ),
                                ],
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )

    search = search.query(query)

    search = search.filter(
        'script',
        script={
            'source': """
                String type = doc.type.value;
                
                if (type == "campaigns") {
                    String campaignName = doc['campaign_name'].value;

                    if (campaignName.length() < 2) { return false; }

                    String objective = campaignName.substring(0,2);
                    return params.objectives.contains(objective);
                } else {
                    String name = doc['order_name'].value;
                    
                    int indexOfObjective = name.indexOf('[O]');
                    
                    if(indexOfObjective != -1) {
                        String objective = name.substring(indexOfObjective + 4, indexOfObjective + 6);
                        return params.objectives.contains(objective);
                    }
                }
                return false;
            """,
            'params': {
                'objectives': [
                    'AM', 'BP', 'CQ', 'DC', 'UZ', 'XM',
                ],
            },
        },
    )

    objectives_aggregation = A(
        'terms',
        size=10,
        script={
            'source': """
                String type = doc.type.value;
                
                if( type == "campaigns" ) {
                    String campaignName = doc['campaign_name'].value;

                    if (campaignName.length() < 2) { return ''; }

                    return campaignName.substring(0,2);
                } else {
                    String name = doc['order_name'].value;
                    
                    int indexOfObjective = name.indexOf('[O]');
                    
                    if(indexOfObjective != -1) {
                        return name.substring(indexOfObjective + 4, indexOfObjective + 6);
                    }
                }
            """,
        },
    ).metric(
        'dsp_sales',
        'sum',
        field='sales_14d',
    ).metric(
        'dsp_spend',
        'sum',
        field='total_cost',
    ).metric(
        'dps_total_sales',
        'sum',
        field='total_sales_14d',
    ).metric(
        'sa_sales',
        'sum',
        field='attributed_sales_14d_same_SKU',
    ).metric(
        'sa_spend',
        'sum',
        field='cost',
    ).metric(
        'sa_total_sales',
        'sum',
        field='attributed_sales_14d',
    )
    
    search.aggs.bucket(
        'current_period',
        'date_range',
        field='report_date',
        ranges={
            'from': start_date,
            'to': end_date,
        },
    )
    search.aggs['current_period'].bucket(
        'objectives',
        objectives_aggregation,
    )

    search.aggs['current_period'].metric(
        'dsp_total_attributed_sales',
        'sum',
        field='total_sales_14d',
    ).metric(
        'dsp_total_sales',
        'sum',
        field='sales_14d',
    ).metric(
        'dsp_total_spend',
        'sum',
        field='total_cost',
    ).metric(
        'sa_total_attributed_sales',
        'sum',
        field='attributed_sales_14d',
    ).metric(
        'sa_total_sales',
        'sum',
        field='attributed_sales_14d_same_SKU',
    ).metric(
        'sa_total_spend',
        'sum',
        field='cost',
    ).metric(
        'total_attributed_sales',
        'bucket_script',
        buckets_path={
            'dsp_sales': 'dsp_total_attributed_sales',
            'sa_sales': 'sa_total_attributed_sales',
        },
        script='params.dsp_sales + params.sa_sales',
    ).metric(
        'total_sales',
        'bucket_script',
        buckets_path={
            'dsp_sales': 'dsp_total_sales',
            'sa_sales': 'sa_total_sales',
        },
        script='params.dsp_sales + params.sa_sales',
    ).metric(
        'total_spend',
        'bucket_script',
        buckets_path={
            'dsp_spend': 'dsp_total_spend',
            'sa_spend': 'sa_total_spend',
        },
        script='params.dsp_spend + params.sa_spend',
    )

    search.aggs.bucket(
        'previous_period',
        'date_range',
        field='report_date',
        ranges={
            'from': previous_start_date,
            'to': previous_end_date,
        },
    ).metric(
        'dsp_total_attributed_sales',
        'sum',
        field='total_sales_14d',
    ).metric(
        'dsp_total_sales',
        'sum',
        field='sales_14d',
    ).metric(
        'dsp_total_spend',
        'sum',
        field='total_cost',
    ).metric(
        'sa_total_attributed_sales',
        'sum',
        field='attributed_sales_14d',
    ).metric(
        'sa_total_sales',
        'sum',
        field='attributed_sales_14d_same_SKU',
    ).metric(
        'sa_total_spend',
        'sum',
        field='cost',
    ).metric(
        'total_attributed_sales',
        'bucket_script',
        buckets_path={
            'dsp_sales': 'dsp_total_attributed_sales',
            'sa_sales': 'sa_total_attributed_sales',
        },
        script='params.dsp_sales + params.sa_sales',
    ).metric(
        'total_sales',
        'bucket_script',
        buckets_path={
            'dsp_sales': 'dsp_total_sales',
            'sa_sales': 'sa_total_sales',
        },
        script='params.dsp_sales + params.sa_sales',
    ).metric(
        'total_spend',
        'bucket_script',
        buckets_path={
            'dsp_spend': 'dsp_total_spend',
            'sa_spend': 'sa_total_spend',
        },
        script='params.dsp_spend + params.sa_spend',
    )
    
    return search.execute()
    

def total_attributed_sales_and_spend_aggregation(client, index, start_date, end_date):
    search = Search(
        using=client,
        index=index,
    )

    query = Q(
        'bool',
        minimum_should_match=1,
        should=[
            Q(
                'term',
                type={
                    'value': 'campaign',
                },
            ),
            Q(
                'term',
                type={
                    'value': 'campaigns',
                },
            ),
        ],
        must=[
            Q(
                'bool',
                minimum_should_match=1,
                should=[
                    Q(
                        'term',
                        segment={
                            'value': 'null',
                        },
                    ),
                    Q(
                        'bool',
                        minimum_should_match=1,
                        should=[
                            Q(
                                'bool',
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                    Q(
                                        'exists',
                                        field='dimension',
                                    ),
                                ],
                            ),
                            Q(
                                'bool',
                                must=[
                                    Q(
                                        'term',
                                        dimension={
                                            'value': 'order',
                                        },
                                    ),
                                ],
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                ],
                            )
                        ]
                    )
                ]
            ),
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        ]
    )

    search = search.query(query)

    search.aggs.metric(
        'total_attributed_sales',
        'sum',
        script={
            'source': """
                String type = doc.type.value;
                if (type == "campaigns") {
                    return doc['attributed_sales_14d'];
                } else {
                    return doc['total_sales_14d'];
                }
            """
        },
    ).metric(
        'total_sales',
        'sum',
        script={
            'source': """
                String type = doc.type.value;
                if (type == "campaigns") {
                    return doc['attributed_sales_14d_same_SKU'];
                } else {
                    return doc['sales_14d'];
                }
            """
        },
    ).metric(
        'total_spend',
        'sum',
        script={
            'source': """
                String type = doc.type.value;
                if (type == "campaigns") {
                    return doc['cost'];
                } else {
                    return doc['total_cost'];
                }
            """
        },
    )

    return search.execute()


def total_attributed_sales_and_spend_detail_aggregation(client, index, start_date, end_date):
    search = Search(
        using=client,
        index=index,
    )
    query = Q(
        'bool',
        minimum_should_match=1,
        should=[
            Q(
                'term',
                type={
                    'value': 'campaign',
                },
            ),
            Q(
                'term',
                type={
                    'value': 'campaigns',
                },
            ),
        ],
        must=[
            Q(
                'bool',
                minimum_should_match=1,
                should=[
                    Q(
                        'term',
                        segment={
                            'value': 'null',
                        },
                    ),
                    Q(
                        'bool',
                        minimum_should_match=1,
                        should=[
                            Q(
                                'bool',
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                    Q(
                                        'exists',
                                        field='dimension',
                                    ),
                                ],
                            ),
                            Q(
                                'bool',
                                must=[
                                    Q(
                                        'term',
                                        dimension={
                                            'value': 'order',
                                        },
                                    ),
                                ],
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                ]
                            )
                        ]
                    )
                ]
            ),
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        ]
    )

    search = search.query(query)
    
    sponsored_ads_aggregation = A(
        'terms',
        size=10,
        script={
            'source': """
                String type = doc.type.value;
                if (type == "campaigns") {
                    return doc['ad_type'];
                } else {
                    return 'dsp';
                }
            """,
        },
    ).metric(
        'sales',
        'sum',
        script={
            'source': """
                String type = doc.type.value;
                if (type == "campaigns") {
                    return doc['attributed_sales_14d_same_SKU'];
                } else {
                    return doc['sales_14d'];
                }
            """
        },
    ).metric(
        'spend',
        'sum',
        script={
            'source': """
                String type = doc.type.value;
                if (type == "campaigns") {
                    return doc['cost'];
                } else {
                    return doc['total_cost'];
                }
            """
        },
    ).metric(
        'total_sales',
        'sum',
        script={
            'source': """
                String type = doc.type.value;
                if (type == "campaigns") {
                    return doc['attributed_sales_14d'];
                } else {
                    return doc['total_sales_14d'];
                }
            """
        },
    )

    search.aggs.bucket(
        'sponsored_ads',
        sponsored_ads_aggregation
    )
    search.aggs.metric(
        'total_attributed_sales',
        'sum',
        script={
            'source': """
                String type = doc.type.value;
                if (type == "campaigns") {
                    return doc['attributed_sales_14d'];
                } else {
                    return doc['total_sales_14d'];
                }
            """
        },
    ).metric(
        'total_sales',
        'sum',
        script={
            'source': """
                String type = doc.type.value;
                if (type == "campaigns") {
                    return doc['attributed_sales_14d_same_SKU'];
                } else {
                    return doc['sales_14d'];
                }
            """
        },
    ).metric(
        'total_spend',
        'sum',
        script={
            'source': """
                String type = doc.type.value;
                if (type == "campaigns") {
                    return doc['cost'];
                } else {
                    return doc['total_cost'];
                }
            """
        },
    )
    
    return search.execute()


# Retail

def brand_analytics_statistics(client, index, distributor_view, report_type, selling_program, start_date, end_date):
    search = Search(
        using=client,
        index=index,
    )

    query = Q(
        'bool',
        must=[
            Q(
                'term',
                type={
                    'value': report_type.value,
                },
            ),
            Q(
                'term',
                sellingProgramName={
                    'value': selling_program.value,
                },
            ),
            Q(
                'term',
                distributorView={
                    'value': distributor_view.value,
                },
            ),
            Q(
                'range',
                reportDate={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        ]
    )

    search = search.query(query)

    asins_aggregation = A(
        'terms',
        field='asin',
        size=1000,
    ).metric(
        'glanceViews',
        'sum',
        field='glanceViews',
    ).metric(
        'shippedCOGS',
        'sum',
        field='shippedCOGS',
    ).metric(
        'shippedRevenue',
        'sum',
        field='shippedRevenue',
    ).metric(
        'shippedUnits',
        'sum',
        field='shippedUnits',
    ).metric(
        'orderedRevenue',
        'sum',
        field='orderedRevenue',
    ).metric(
        'orderedUnits',
        'sum',
        field='orderedUnits',
    )

    search.aggs.bucket(
        'asins',
        asins_aggregation,
    )
    
    return search.execute()


def brand_analytics_time_series(client, index, distributor_view, report_type, selling_program, start_date, end_date, interval):
    search = Search(
        using=client,
        index=index,
    )

    query = Q(
        'bool',
        must=[
            Q(
                'term',
                type={
                    'value': report_type.value,
                },
            ),
            Q(
                'term',
                sellingProgramName={
                    'value': selling_program.value,
                },
            ),
            Q(
                'term',
                distributorView={
                    'value': distributor_view.value,
                },
            ),
            Q(
                'range',
                reportDate={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        ]
    )

    search = search.query(query)

    search.aggs.bucket(
        'interval',
        'date_histogram',
        extended_bounds={
            'min': start_date,
            'max': end_date,
        },
        field='reportDate',
        interval=interval,
        min_doc_count=0,
    )

    asins_aggregation = A(
        'terms',
        field='asin',
        size=1000,
    ).metric(
        'glanceViews',
        'sum',
        field='glanceViews',
    ).metric(
        'shippedCOGS',
        'sum',
        field='shippedCOGS',
    ).metric(
        'shippedRevenue',
        'sum',
        field='shippedRevenue',
    ).metric(
        'shippedUnits',
        'sum',
        field='shippedUnits',
    ).metric(
        'orderedRevenue',
        'sum',
        field='orderedRevenue',
    ).metric(
        'orderedUnits',
        'sum',
        field='orderedUnits',
    )

    search.aggs['interval'].bucket(
        'asins',
        asins_aggregation,
    )
    
    return search.execute()


def search_terms_time_series(client, index, data):
    search = Search(
        using=client,
        index=index,
    )

    query = Q(
        'bool',
        filter=[
            Q(
                'term',
                report_range=data.interval.value,
            ),
            Q(
                'range',
                report_date={
                    'gte': data.start_date,
                    'lte': data.end_date,
                },
            ),
        ],
    )

    search = search.query(query)

    for search_term in data.search_terms:
        aggregation = A(
            'filter',
            Q(
                'term',
                search_term_search__keyword=search_term,
            )
        )
        aggregation.bucket(
            'ranking_over_time',
            'date_histogram',
            field='report_date',
            interval='week',
            format='yyyy-MM',
            min_doc_count=0,
        )
        aggregation['ranking_over_time'].bucket(
            'ranking',
            'terms',
            field='search_frequency_rank',
            size=1,
        )

        search.aggs.bucket(
            search_term,
            aggregation,
        )

    return search.execute()


def search_terms_filter(client, index, q, limit):
    search = Search(
        using=client,
        index=index,
    )

    query = Q(
        'match_phrase_prefix',
        search_term_search=q,
    )

    search = search.query(query)
    search.update_from_dict(
        {
            'collapse': {
                'field': 'search_term_search.keyword',
            },
        },
    )

    return search[0:limit].execute()


def search_terms_periods(client, index):
    search = Search(
        using=client,
        index=index,
    )

    query = Q(
        'term',
        report_range={
            'value': BrandAnalyticsIntervalType.MONTHLY.value,
        },
    )

    search = search.query(query)

    dates_aggregation = A(
        'terms',
        field='report_date',
        size=10000,
    )

    search.aggs.bucket(
        'dates',
        dates_aggregation,
    )

    return search.execute()


def search_terms(client, index, data):
    report_range_components = data.report_range.split(
        Constants.DASH,
    )

    month, year = report_range_components
    first_day_of_month = 1
    last_day_of_month = calendar.monthrange(
        int(year),
        int(month),
    )[1]

    start_date = f'{year}-{month}-01'
    end_date = f'{year}-{month}-{last_day_of_month}'

    search = Search(
        using=client,
        index=index,
    )

    must_filter = []
    if data.asins:
        must_filter = [Q(
            'bool',
            should=[
                Q(
                    'terms',
                    first_clicked_asin=data.asins,
                ),
                Q(
                    'terms',
                    second_clicked_asin=data.asins,
                ),
                Q(
                    'terms',
                    third_clicked_asin=data.asins,
                ),
            ],
        )]
        
    must_not_filter = []
    if data.exclude:
        for search_term in data.exclude:
            must_not_filter.append(
                Q(
                    'terms',
                    search_term=[search_term],
                )
            )

    should_filter = [
        Q(
            'match_phrase_prefix',
            search_term_search=search_term,
        )
        for search_term in data.search_terms
    ]

    for product_title in data.product_titles:
        should_filter.append(
            Q(
                'match_phrase',
                first_product_title=product_title,
            )
        )
        should_filter.append(
            Q(
                'match_phrase',
                second_product_title=product_title,
            )
        )
        should_filter.append(
            Q(
                'match_phrase',
                third_product_title=product_title,
            )
        )

    query = Q(
        'bool',
        minimum_should_match=1,
        filter=[
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
            Q(
                'term',
                report_range=BrandAnalyticsIntervalType.MONTHLY.value,
            ),
        ],
        must=must_filter,
        must_not=must_not_filter,
        should=should_filter,
    )

    search = search.query(query)

    search = search.sort(
        {'search_frequency_rank': 'asc'},
    )
    
    return search[0:1000].execute()


# Tags

def dsp_and_sa_tags_time_series(client, index, campaign_ids, order_ids, start_date, end_date, interval, objectives, segments):
    search = Search(
        using=client,
        index=index,
    )
    query = Q(
        'bool',
        minimum_should_match=1,
        should=[
            Q(
                'term',
                type={
                    'value': 'campaign',
                },
            ),
            Q(
                'term',
                type={
                    'value': 'campaigns',
                },
            ),
        ],
        must=[
            Q(
                'bool',
                minimum_should_match=1,
                should=[
                    Q(
                        'terms',
                        campaign_id=campaign_ids,
                    ),
                    Q(
                        'terms',
                        order_id=order_ids,
                    ),
                ],
            ),
            Q(
                'bool',
                minimum_should_match=1,
                should=[
                    Q(
                        'term',
                        segment={
                            'value': 'null',
                        },
                    ),
                    Q(
                        'bool',
                        minimum_should_match=1,
                        should=[
                            Q(
                                'bool',
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                    Q(
                                        'exists',
                                        field='dimension',
                                    ),
                                ],
                            ),
                            Q(
                                'bool',
                                must=[
                                    Q(
                                        'term',
                                        dimension={
                                            'value': 'order',
                                        },
                                    ),
                                ],
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        ],
    )

    search = search.query(query)

    if objectives or segments:
        search = search.filter(
            Q(
                'script',
                script={
                    'source': """
                            String type = doc.type.value;
                            if (type == "campaigns") {
                                String campaignName = doc['campaign_name'].value;

                                if (campaignName.length() < 2) { return false; }

                                String objective = campaignName.substring(0,2);
                                return params.objectives.contains(objective);
                            } else {
                                String name = doc['order_name'].value;

                                int indexOfFunnel = name.indexOf('[F]');
                                int indexOfObjective = name.indexOf('[O]');

                                if (indexOfFunnel != -1 || indexOfObjective != -1) {
                                    String objective = name.substring(indexOfObjective + 4, indexOfObjective + 6);
                                    String funnel = name.substring(indexOfFunnel + 4, indexOfFunnel + 6);
                                    return params.objectives.contains(objective) || params.funnels.contains(funnel);
                                }
                            }
                            return false;
                        """,
                    'params': {
                        'funnels': segments,
                        'objectives': objectives,
                    },
                }
            )
        )

    search.aggs.bucket(
        'interval',
        'date_histogram',
        extended_bounds={
            'min': start_date,
            'max': end_date,
        },
        field='report_date',
        interval=interval,
        min_doc_count=0,
    ).metric(
        'sa_clicks',
        'sum',
        field='clicks',
    ).metric(
        'dsp_clicks',
        'sum',
        field='click_throughs',
    ).metric(
        'sa_dpv',
        'sum',
        field='attributed_dpv_14d',
    ).metric(
        'dsp_dpv',
        'sum',
        field='dpv_14d',
    ).metric(
        'sa_sales',
        'sum',
        field='attributed_sales_14d_same_SKU',
    ).metric(
        'dsp_sales',
        'sum',
        field='sales_14d',
    ).metric(
        'sa_spend',
        'sum',
        field='cost',
    ).metric(
        'dsp_spend',
        'sum',
        field='total_cost',
    ).metric(
        'sa_total_ntb_sales',
        'sum',
        field='attributed_sales_new_to_brand_14d',
    ).metric(
        'dsp_total_ntb_sales',
        'sum',
        field='total_new_to_brand_product_sales_14d',
    ).metric(
        'sa_total_product_sales',
        'sum',
        field='attributed_sales_14d',
    ).metric(
        'dsp_total_product_sales',
        'sum',
        field='total_sales_14d',
    ).metric(
        'sa_attributed_conversions_14d',
        'sum',
        field='attributed_conversions_14d',
    ).metric(
        'dsp_purchases_14d',
        'sum',
        field='purchases_14d',
    ).metric(
        'cpa',
        'bucket_script',
        buckets_path={
            'sa_cost': 'sa_spend',
            'dsp_cost': 'dsp_spend',
            'sa_conversions': 'sa_attributed_conversions_14d',
            'dsp_purchases': 'dsp_purchases_14d',
        },
        script='((params.sa_cost + params.dsp_cost)/(params.sa_conversions + params.dsp_purchases))',
        gap_policy='insert_zeros',
    ).metric(
        'total_impressions',
        'sum',
        field='impressions',
    ).metric(
        'ctr',
        'bucket_script',
        buckets_path={
            'sa_clicks': 'sa_clicks',
            'dsp_clicks': 'dsp_clicks',
            'impressions': 'total_impressions',
        },
        script='((params.sa_clicks + params.dsp_clicks)/params.impressions) * 100',
        gap_policy='insert_zeros',
    ).metric(
        'dsp_dpvr',
        'avg',
        field='dpvr_14d',
    ).metric(
        'sa_impressions',
        'sum',
        script={
            'source': """
                if (doc.type.value == 'campaigns') return doc.impressions.value;
            """
        },
    ).metric(
        'dsp_impressions',
        'sum',
        script={
            'source': """
                if (doc.type.value == 'campaign') return doc.impressions.value;
            """
        },
    ).metric(
        'sa_units_sold',
        'sum',
        script={
            'source': """
                if (doc.type.value == 'campaigns') return doc.units_sold_14d.value;
            """
        },
    ).metric(
        'dsp_units_sold',
        'sum',
        script={
            'source': """
                if (doc.type.value == 'campaign') return doc.units_sold_14d.value;
            """
        },
    ).metric(
        'sa_total_units_sold',
        'sum',
        field='attributed_units_sold_14d',
    ).metric(
        'dsp_total_units_sold',
        'sum',
        field='total_units_sold_14d'
    ).metric(
        'dpvr',
        'bucket_script',
        buckets_path={
            'dsp_dpv': 'dsp_dpv',
            'sa_dpv': 'sa_dpv',
            'impressions': 'total_impressions',
        },
        script='((params.sa_dpv + params.dsp_dpv)/params.impressions)',
        gap_policy='insert_zeros',
    ).metric(
        'dsp_ecpm',
        'avg',
        field='e_cpm',
    ).metric(
        'ecpm',
        'bucket_script',
        buckets_path={
            'dsp_ecpm': 'dsp_ecpm',
            'dsp_cost': 'dsp_spend',
            'dsp_impressions': 'dsp_impressions',
            'sa_cost': 'sa_spend',
            'sa_impressions': 'sa_impressions',
        },
        script='((params.sa_cost + params.dsp_cost)/(params.sa_impressions + params.dsp_impressions)) * 1000',
        gap_policy='insert_zeros',
    ).metric(
        'ntb_percentage',
        'bucket_script',
        buckets_path={
            'sa_ntb_sales': 'sa_total_ntb_sales',
            'dsp_ntb_sales': 'dsp_total_ntb_sales',
            'sa_sales': 'sa_sales',
            'dsp_sales': 'dsp_sales',
        },
        script='((params.sa_ntb_sales + params.dsp_ntb_sales)/(params.sa_sales + params.dsp_sales)) * 100',
        gap_policy='insert_zeros',
    ).metric(
        'total_ntb_percentage',
        'bucket_script',
        buckets_path={
            'sa_ntb_sales': 'sa_total_ntb_sales',
            'dsp_ntb_sales': 'dsp_total_ntb_sales',
            'sa_total_sales': 'sa_total_product_sales',
            'dsp_total_sales': 'dsp_total_product_sales',
        },
        script='((params.sa_ntb_sales + params.dsp_ntb_sales)/(params.sa_total_sales + params.dsp_total_sales)) * 100',
        gap_policy='insert_zeros',
    ).metric(
        'acpc',
        'bucket_script',
        buckets_path={
            'sa_spend': 'sa_spend',
            'dsp_spend': 'dsp_spend',
            'sa_clicks': 'sa_clicks',
            'dsp_clicks': 'dsp_clicks',
        },
        script='(params.sa_spend + params.dsp_spend)/(params.sa_clicks + params.dsp_clicks)',
        gap_policy='insert_zeros',
    ).metric(
        'total_roas',
        'bucket_script',
        buckets_path={
            'sa_sales': 'sa_total_product_sales',
            'dsp_sales': 'dsp_total_product_sales',
            'sa_spend': 'sa_spend',
            'dsp_spend': 'dsp_spend',
        },
        script='(params.sa_sales + params.dsp_sales)/(params.sa_spend + params.dsp_spend)',
        gap_policy='insert_zeros',
    ).metric(
        'roas',
        'bucket_script',
        buckets_path={
            'sa_sales': 'sa_sales',
            'dsp_sales': 'dsp_sales',
            'sa_spend': 'sa_spend',
            'dsp_spend': 'dsp_spend',
        },
        script='(params.sa_sales + params.dsp_sales)/(params.sa_spend + params.dsp_spend)',
        gap_policy='insert_zeros',
    )

    return search.execute()


def dsp_tags_time_series(client, index, order_ids, start_date, end_date, interval, objectives, segments):
    search = Search(
        using=client,
        index=index,
    )
    query = Q(
        'bool',
        must=[
            Q(
                'terms',
                order_id=order_ids,
            ),
            Q(
                'term',
                type={
                    'value': 'campaign',
                },
            ),
            Q(
                'term',
                dimension={
                    'value': 'order',
                },
            ),
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        ],
    )

    search = search.query(query)

    if objectives or segments:
        search = search.filter(
            Q(
                'script',
                script={
                    'source': """
                            String name = doc['order_name'].value;

                            int indexOfFunnel = name.indexOf('[F]');
                            int indexOfObjective = name.indexOf('[O]');

                            if (indexOfFunnel != -1 || indexOfObjective != -1) {
                                String objective = name.substring(indexOfObjective + 4, indexOfObjective + 6);
                                String funnel = name.substring(indexOfFunnel + 4, indexOfFunnel + 6);
                                return params.objectives.contains(objective) || params.funnels.contains(funnel);
                            }
                        """,
                    'params': {
                        'funnels': segments,
                        'objectives': objectives,
                    },
                }
            )
        )

    search.aggs.bucket(
        'interval',
        'date_histogram',
        extended_bounds={
            'min': start_date,
            'max': end_date,
        },
        field='report_date',
        interval=interval,
        min_doc_count=0,
    ).metric(
        'clicks',
        'sum',
        field='click_throughs',
    ).metric(
        'dpv',
        'sum',
        field='dpv_14d',
    ).metric(
        'impressions',
        'sum',
        field='impressions',
    ).metric(
        'sales',
        'sum',
        field='sales_14d',
    ).metric(
        'spend',
        'sum',
        field='total_cost',
    ).metric(
        'total_ntb_sales',
        'sum',
        field='total_new_to_brand_product_sales_14d',
    ).metric(
        'total_product_sales',
        'sum',
        field='total_sales_14d',
    ).metric(
        'purchases',
        'sum',
        field='purchases_14d',
    ).metric(
        'units_sold',
        'sum',
        field='units_sold_14d',
    ).metric(
        'total_units_sold',
        'sum',
        field='total_units_sold_14d',
    ).metric(
        'cpa',
        'bucket_script',
        buckets_path={
            'cost': 'spend',
            'purchases': 'purchases',
        },
        script='params.cost/params.purchases',
        gap_policy='insert_zeros',
    ).metric(
        'ctr',
        'bucket_script',
        buckets_path={
            'clicks': 'clicks',
            'impressions': 'impressions',
        },
        script='(params.clicks/params.impressions) * 100',
        gap_policy='insert_zeros',
    ).metric(
        'dpvr',
        'avg',
        field='dpvr_14d',
    ).metric(
        'ecpm',
        'avg',
        field='e_cpm',
    ).metric(
        'ntb_percentage',
        'bucket_script',
        buckets_path={
            'ntb_sales': 'total_ntb_sales',
            'sales': 'sales',
        },
        script='((params.ntb_sales / params.sales) * 100)',
        gap_policy='insert_zeros',
    ).metric(
        'acpc',
        'bucket_script',
        buckets_path={
            'clicks': 'clicks',
            'spend': 'spend',
        },
        script='params.spend/params.clicks',
        gap_policy='insert_zeros',
    ).metric(
        'total_ntb_percentage',
        'bucket_script',
        buckets_path={
            'ntb_sales': 'total_ntb_sales',
            'total_sales': 'total_product_sales',
        },
        script='((params.ntb_sales / params.total_sales) * 100)',
        gap_policy='insert_zeros',
    ).metric(
        'total_roas',
        'bucket_script',
        buckets_path={
            'sales': 'total_product_sales',
            'spend': 'spend',
        },
        script='params.sales/params.spend',
        gap_policy='insert_zeros',
    ).metric(
        'roas',
        'bucket_script',
        buckets_path={
            'sales': 'sales',
            'spend': 'spend',
        },
        script='params.sales/params.spend',
        gap_policy='insert_zeros',
    )

    return search.execute()


def sa_tags_time_series(client, index, campaign_ids, start_date, end_date, interval, objectives):
    search = Search(
        using=client,
        index=index,
    )
    query = Q(
        'bool',
        must=[
            Q(
                'terms',
                campaign_id=campaign_ids,
            ),
            Q(
                'bool',
                minimum_should_match=1,
                should=[
                    Q(
                        'term',
                        segment={
                            'value': 'null',
                        },
                    ),
                    Q(
                        'bool',
                        must_not=[
                            Q(
                                'exists',
                                field='segment',
                            ),
                        ],
                    ),
                ],
            ),
            Q(
                'term',
                type={
                    'value': 'campaigns',
                },
            ),
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        ],
    )

    search = search.query(query)

    if objectives:
        search = search.filter(
            Q(
                'script',
                script={
                    'source': """
                            String campaignName = doc['campaign_name'].value;

                            if (campaignName.length() < 2) { return false; }

                            String objective = campaignName.substring(0, 2);
                            return params.objectives.contains(objective);
                        """,
                    'params': {
                        'objectives': objectives,
                    },
                }
            )
        )

    search.aggs.bucket(
        'interval',
        'date_histogram',
        extended_bounds={
            'min': start_date,
            'max': end_date,
        },
        field='report_date',
        interval=interval,
        min_doc_count=0,
    ).metric(
        'clicks',
        'sum',
        field='clicks',
    ).metric(
        'dpv',
        'sum',
        field='attributed_dpv_14d',
    ).metric(
        'impressions',
        'sum',
        field='impressions',
    ).metric(
        'sales',
        'sum',
        field='attributed_sales_14d_same_SKU',
    ).metric(
        'spend',
        'sum',
        field='cost',
    ).metric(
        'total_ntb_sales',
        'sum',
        field='attributed_sales_new_to_brand_14d',
    ).metric(
        'total_product_sales',
        'sum',
        field='attributed_sales_14d',
    ).metric(
        'conversions',
        'sum',
        field='attributed_conversions_14d',
    ).metric(
        'units_sold',
        'sum',
        field='units_sold_14d',
    ).metric(
        'total_units_sold',
        'sum',
        field='attributed_units_sold_14d',
    ).metric(
        'cpa',
        'bucket_script',
        buckets_path={
            'cost': 'spend',
            'conversions': 'conversions',
        },
        script='params.cost/params.conversions',
        gap_policy='insert_zeros',
    ).metric(
        'ctr',
        'bucket_script',
        buckets_path={
            'clicks': 'clicks',
            'impressions': 'impressions',
        },
        script='(params.clicks/params.impressions) * 100',
        gap_policy='insert_zeros',
    ).metric(
        'dpvr',
        'bucket_script',
        buckets_path={
            'dpv': 'dpv',
            'impressions': 'impressions',
        },
        script='params.dpv/params.impressions',
        gap_policy='insert_zeros',
    ).metric(
        'ecpm',
        'bucket_script',
        buckets_path={
            'cost': 'spend',
            'impressions': 'impressions',
        },
        script='(params.cost/params.impressions) * 1000',
        gap_policy='insert_zeros',
    ).metric(
        'acpc',
        'bucket_script',
        buckets_path={
            'clicks': 'clicks',
            'spend': 'spend',
        },
        script='params.spend/params.clicks',
        gap_policy='insert_zeros',
    ).metric(
        'total_roas',
        'bucket_script',
        buckets_path={
            'sales': 'total_product_sales',
            'spend': 'spend',
        },
        script='params.sales/params.spend',
        gap_policy='insert_zeros',
    ).metric(
        'roas',
        'bucket_script',
        buckets_path={
            'sales': 'sales',
            'spend': 'spend',
        },
        script='params.sales/params.spend',
        gap_policy='insert_zeros',
    )
    
    return search.execute()


def tag_statistics(client, index, campaign_ids, order_ids, start_date, end_date, objectives, segments):
    search = Search(
        using=client,
        index=index,
    )
    query = Q(
        'bool',
        minimum_should_match=1,
        should=[
            Q(
                'term',
                type={
                    'value': 'campaign',
                },
            ),
            Q(
                'term',
                type={
                    'value': 'campaigns',
                },
            ),
        ],
        must=[
            Q(
                'bool',
                minimum_should_match=1,
                should=[
                    Q(
                        'terms',
                        campaign_id=campaign_ids,
                    ),
                    Q(
                        'terms',
                        order_id=order_ids,
                    ),
                ],
            ),
            Q(
                'bool',
                minimum_should_match=1,
                should=[
                    Q(
                        'term',
                        segment={
                            'value': 'null',
                        },
                    ),
                    Q(
                        'bool',
                        minimum_should_match=1,
                        should=[
                            Q(
                                'bool',
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                    Q(
                                        'exists',
                                        field='dimension',
                                    ),
                                ],
                            ),
                            Q(
                                'bool',
                                must=[
                                    Q(
                                        'term',
                                        dimension={
                                            'value': 'order',
                                        },
                                    ),
                                ],
                                must_not=[
                                    Q(
                                        'exists',
                                        field='segment',
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
            Q(
                'range',
                report_date={
                    'gte': start_date,
                    'lte': end_date,
                },
            ),
        ]
    )

    search = search.query(query)

    if objectives or segments:
        search = search.filter(
            Q(
                'script',
                script={
                    'source': """
                        String type = doc.type.value;
                        if (type == "campaigns") {
                            String campaignName = doc['campaign_name'].value;

                            if (campaignName.length() < 2) { return false; }

                            String objective = campaignName.substring(0,2);
                            return params.objectives.contains(objective);
                        } else {
                            String name = doc['order_name'].value;
                        
                            int indexOfFunnel = name.indexOf('[F]');
                            int indexOfObjective = name.indexOf('[O]');
                            
                            if (indexOfFunnel != -1 || indexOfObjective != -1) {
                                String objective = name.substring(indexOfObjective + 4, indexOfObjective + 6);
                                String funnel = name.substring(indexOfFunnel + 4, indexOfFunnel + 6);
                                return params.objectives.contains(objective) || params.funnels.contains(funnel);
                            }
                        }
                        return false;
                    """,
                    'params': {
                        'funnels': segments,
                        'objectives': objectives,
                    },
                }
            )
        )

    items_aggregation = A(
            'terms',
            size=1000,
            script={
                'source': """
                    if (doc.type.value == "campaign") return doc.order_name.value;
                
                    return doc.campaign_name.value;
                """
            },
        ).metric(
            'spend',
            'sum',
            script={
                'source': """
                    if (doc.type.value == "campaign") return doc.total_cost.value;
            
                    return doc.cost.value;
                """
            },
        ).metric(
            'sales',
            'sum',
            script={
                "source": """
                    if (doc.type.value == "campaign") return doc.sales_14d.value;
                
                    return doc.attributed_sales_14d_same_SKU.value;
                """
            },
        ).metric(
            'impressions',
            'sum',
            field='impressions',
        ).metric(
            'clicks',
            'sum',
            script={
                "source": """
                    if (doc.type.value == "campaign") return doc.click_throughs.value;
            
                    return doc.clicks.value;
                """
            }
        ).metric(
            'total_sales',
            'sum',
            script={
                "source": """
                    if (doc.type.value == "campaign") return doc.total_sales_14d.value;
                
                    return doc.attributed_sales_14d.value;
                """
            },
        )
    
    search.aggs.bucket(
        'campaigns',
        items_aggregation,
    )

    return search.execute()


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

    # search_terms_dict = {
    #     'asins': ['B07TZHNN92'],
    #     'exclude': ['pro'],
    #     'product_titles': ['apple'],
    #     'report_range': '07-2021',
    #     'search_terms': [
    #         'airpods',
    #         'airpod',
    #     ]
    # }

    # search_terms_schema = IndexSearchTermsSchema(**search_terms_dict)

    search = my_dashboard_statistics(
        client,
        ['sa_1065597062491154'],
        '2021-10-02',
        '2021-11-01',
        'week',
        TableType.ALL,
    )

    print(
        json.dumps(
            search.to_dict(),
            indent=2,
            sort_keys=False,
        )
    )
