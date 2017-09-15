import requests
import json
from deepdiff import DeepDiff
from toflercommon.basetestutils import BaseTest


class TestGraphQueryV2(BaseTest):

    #validation in valid test queries
        #no count inside count (in case of relational property we can ignore in case of raw_output)
        #no data with sum/avg/count/groupby at same level
        #aggregation on text field throws fieldata error use keyword throws TransportError(400,
            # u'search_phase_execution_exception', u'Fielddata is disabled on text fields by default
        #aggregation sum/avg on non numeric field throws (400, u'search_phase_execution_exception',
            # u'Expected numeric type on field [tsales:userId.value], but got [keyword]')
        #aggregation of  count filter is not supported
    #Todo
        # having count()>= 20
        # include all fields at same level in min/max
        # group by include fn related data
    #optimize
        #multiple orders with same buyer multiple requests are made
        #filters and query separate for filter cache
        #reverse relationship
    #fixes
    # test case 2 key random
    # test case 3 all count coming 6

    test_config = [
        {
            #same level with filters inside / outside
            #fill data at outer
            #with single entity filter result should be only one bucket
            'input': {
                'id': 'dc68gb91ktqcwb',
                'go:type': 'tsales:ToflerOrder',
                'tsales:totalAmount': None,
                'tsales:toflerProduct': {
                    'sum(tsales:price)': None,
                    'count(tsales:toflerProductCIN)': None,
                    'tsales:price<=': 150,
                },
            },
            'output': {
                "status": "SUCCESS",
                "content": [
                    {
                        "tsales:totalAmount": [
                            1174.0
                        ],
                        "go:type": [
                            [
                                "go:Entity",
                                "tsales:ToflerOrder"
                            ]
                        ],
                        "id": "dc68gb91ktqcwb",
                        "tsales:toflerProduct": {
                            "count(tsales:toflerProductCIN)": [
                                {
                                    "value": 5
                                }
                            ],
                            "sum(tsales:price)": [
                                {
                                    "value": 1174.0
                                }
                            ]
                        }
                    }
                ]
            },
        },
        {
            #same level with filters inside / outside
            #fill data at outer
            #multiple buckets
            'input': {
                'id': None,
                'go:type': 'tsales:ToflerOrder',
                'tsales:totalAmount': None,
                'tsales:toflerProduct': {
                    'sum(tsales:price)': None,
                    'tsales:price<=': 150,
                },
            },
            'output': {}
            # {
            #     "root": {
            #         "buckets": [
            #             {
            #                 "tsales:toflerProduct-nested": {
            #                     "tsales:toflerProduct.tsales:price.value_sum": {
            #                         "value": 116.0
            #                     },
            #                     "doc_count": 1
            #                 },
            #                 "key": "dc679092h0sgq2",
            #                 "doc_count": 1
            #             },
            #             {
            #                 "tsales:toflerProduct-nested": {
            #                     "tsales:toflerProduct.tsales:price.value_sum": {
            #                         "value": 116.0
            #                     },
            #                     "doc_count": 1
            #                 },
            #                 "key": "dc6791cp4cwd28",
            #                 "doc_count": 1
            #             },
            #         ],
            #         "doc_count_error_upper_bound": 10,
            #         "sum_other_doc_count": 20462
            #     }
            # }
        },
        {
            #filter then count
            #ignore  filters at count level in output
            'input': {
                'go:type': 'tsales:ToflerOrder',
                'tsales:totalAmount>=': 2300,
                'tsales:toflerProduct': {
                    'count(tsales:toflerProductCIN)': None,
                    'tsales:deliveredStatus': 'DELIVERED',
                },
            },
            'output': {}
            # {
            #     "root": {
            #         "buckets": [
            #             {
            #                 "tsales:toflerProduct-nested": {
            #                     "doc_count": 6,
            #                     "tsales:toflerProduct.tsales:toflerProductCIN.value_count": {
            #                         "value": 6
            #                     }
            #                 },
            #                 "key": "dc680558rbddy3",
            #                 "doc_count": 1
            #             },
            #             {
            #                 "tsales:toflerProduct-nested": {
            #                     "doc_count": 1,
            #                     "tsales:toflerProduct.tsales:toflerProductCIN.value_count": {
            #                         "value": 1
            #                     }
            #                 },
            #                 "key": "dc6808y8dzvg8h",
            #                 "doc_count": 1
            #             },
            #         ],
            #         "doc_count_error_upper_bound": 10,
            #         "sum_other_doc_count": 363
            #     }
            # }
        },
        {
            #count then filter
            'input': {
                'go:type': 'tsales:ToflerOrder',
                'sum(tsales:totalAmount)': None,
                'tsales:toflerProduct': {
                    'tsales:deliveredStatus': 'DELIVERED',
                },
            },
            'output': {}
            # {
            #     u'tsales:totalAmount.value_sum': {
            #         u'value': 8833403.0
            #     }
            # }
        },
        {
            #min then filter
            'input': {
                'go:type': 'tsales:ToflerOrder',
                'min(tsales:totalAmount)': None,
                'tsales:toflerProduct': {
                    'tsales:deliveredStatus': 'DELIVERED',
                },
            },
            'output': {}
            # {
            #     "tsales:totalAmount.value_min": {
            #         "hits": {
            #             "hits": [
            #                 {
            #                     "sort": [
            #                         79.0
            #                     ],
            #                     "_type": "node_devel",
            #                     "_index": "toflerdb_snapshot_devel",
            #                     "_score": null,
            #                     "inner_hits" : {//somedata}
            #                     "_source": {
            #                         "tsales:totalAmount": [
            #                             {
            #                                 "value": 79.0
            #                             }
            #                         ]
            #                     },
            #                     "_id": "dc68jvf5rvt7gy"
            #                 }
            #             ],
            #             "total": 23103,
            #             "max_score": null
            #         }
            #     }
            # }
        },
        {
            #filter then max
            'input': {
                'go:type': 'tsales:ToflerOrder',
                'tsales:totalAmount>=': 1000,
                'tsales:toflerProduct': {
                    'max(tsales:price)': None,
                    'tsales:deliveredStatus': "DELIVERED"
                }
            },
            'output': {}
            # {
            #     "root": {
            #         "buckets": [
            #             {
            #                 "tsales:toflerProduct-nested": {
            #                     "tsales:toflerProduct.tsales:price.value_max": {
            #                         "hits": {
            #                             "hits": [
            #                                 {
            #                                     "sort": [
            #                                         1477.0
            #                                     ],
            #                                     "_nested": {
            #                                         "field": "tsales:toflerProduct",
            #                                         "offset": 1
            #                                     },
            #                                     "inner_hits": {
            #                                         "tsales:toflerProduct": {
            #                                             "hits": {
            #                                                 "hits": [],
            #                                                 "total": 0,
            #                                                 "max_score": null
            #                                             }
            #                                         }
            #                                     },
            #                                     "_score": null,
            #                                     "_source": {
            #                                         "tsales:toflerProduct": {
            #                                             "tsales:price": [
            #                                                 {
            #                                                     "value": 1477.0
            #                                                 }
            #                                             ]
            #                                         }
            #                                     }
            #                                 }
            #                             ],
            #                             "total": 2,
            #                             "max_score": null
            #                         }
            #                     },
            #                     "doc_count": 2
            #                 },
            #                 "key": "dc6802tqc1znvj",
            #                 "doc_count": 1
            #             }
            #             {
            #                 "tsales:toflerProduct-nested": {
            #                     "tsales:toflerProduct.tsales:price.value_max": {
            #                         "hits": {
            #                             "hits": [
            #                                 {
            #                                     "sort": [
            #                                         1477.0
            #                                     ],
            #                                     "_nested": {
            #                                         "field": "tsales:toflerProduct",
            #                                         "offset": 0
            #                                     },
            #                                     "inner_hits": {
            #                                         "tsales:toflerProduct": {
            #                                             "hits": {
            #                                                 "hits": [
            #                                                     {
            #                                                         "_nested": {
            #                                                             "field": "tsales:toflerProduct",
            #                                                             "offset": 1
            #                                                         },
            #                                                         "_score": 0.034710985,
            #                                                         "_source": {
            #                                                             "tsales:price": [
            #                                                                 {
            #                                                                     "fact_id": "dc6803qwmlv76l",
            #                                                                     "value": 521.0
            #                                                                 }
            #                                                             ],
            #                                                             "tsales:deliveredStatus": [
            #                                                                 {
            #                                                                     "fact_id": "dc6803qxt3jrdm",
            #                                                                     "value": "DELIVERED"
            #                                                                 }
            #                                                             ],
            #                                                             "fact_id": "dc6803qy43562r",
            #                                                             "tsales:productType": [
            #                                                                 {
            #                                                                     "fact_id": "dc6803qvqgwc9t",
            #                                                                     "value": "UnlistedPublicCompanyAllDocs"
            #                                                                 }
            #                                                             ],
            #                                                             "tsales:toflerProductCIN": [
            #                                                                 {
            #                                                                     "fact_id": "dc6803qtgswkyr",
            #                                                                     "value": "dc67ypz5nnsj2d"
            #                                                                 }
            #                                                             ],
            #                                                             "go:type": [
            #                                                                 {
            #                                                                     "fact_id": "dc6803qnvs7k4t",
            #                                                                     "value": [
            #                                                                         "go:ComplexProperty",
            #                                                                         "go:Property",
            #                                                                         "tsales:toflerProduct"
            #                                                                     ]
            #                                                                 }
            #                                                             ],
            #                                                             "id": "dc6803qnbdkzwc"
            #                                                         }
            #                                                     }
            #                                                 ],
            #                                                 "total": 1,
            #                                                 "max_score": 0.034710985
            #                                             }
            #                                         }
            #                                     },
            #                                     "_score": null,
            #                                     "_source": {
            #                                         "tsales:toflerProduct": {
            #                                             "tsales:price": [
            #                                                 {
            #                                                     "value": 1477.0
            #                                                 }
            #                                             ]
            #                                         }
            #                                     }
            #                                 }
            #                             ],
            #                             "total": 2,
            #                             "max_score": null
            #                         }
            #                     },
            #                     "doc_count": 2
            #                 },
            #                 "key": "dc6803krpb83rz",
            #                 "doc_count": 1
            #             }
            #         ],
            #         "doc_count_error_upper_bound": 10,
            #         "sum_other_doc_count": 1742
            #     }
            # }
        },
        {
            #same level with filters inside / outside
            #fill data at outer
            'input': {
                'id': 'dc68c5t7yyw59t',
                'go:type': 'tsales:ToflerOrder',
                'tsales:totalAmount>=': 1000,
                'tsales:creationTime': None,
                'tsales:buyer': {
                    'count(tsales:userId)': None,
                },
            },
            'output': {}
            # {u'tsales:userId.value_count': {u'value': 1}}
        },
        {
            #filter then count
            #ignore  filters at count level in output
            'input': {
                'go:type': 'tsales:ToflerOrder',
                'tsales:totalAmount>=': 1300,
                # 'tsales:creationTime': None,
                'id': None,
                'tsales:buyer': {
                    'count(tsales:userId)': None
                },
            },
            'output': {}
        },
        {
            #filter then max filled inside outside
            'input': {
                'go:type': 'tsales:ToflerOrder',
                'tsales:totalAmount>=': 1000,
                'id': None,
                'tsales:buyer':  {
                    'max(tsales:userId)': None,
                    'id': None
                }
            },
            'output': {}
        },
        {
            #above with filter at same level
            'input': {
                'go:type': 'tsales:ToflerOrder',
                'tsales:totalAmount>=': 1000,
                'id': None,
                'tsales:buyer':  {
                    'max(tsales:userId)': None,
                    'id': None,
                    'tsales:userId': 15429,
                }
            },
            'output': {}
        },
        {
            #above with filter at same level but different entities
            'input': {
                'go:type': 'tsales:ToflerOrder',
                'tsales:totalAmount>=': 1000,
                'id': None,
                'tsales:buyer':  {
                    'max(tsales:userId)': None,
                    'id': None,
                    'tsales:userId': 15429,
                },
                'tsales:toflerProduct': {
                    'count(tsales:toflerProductCIN)': None,
                    'tsales:deliveredStatus': 'DELIVERED',
                },
            },
            'output': {}
        },
        {
            #filter then groupby
            #ignore  filters at groupby level in output
            'input': {
                'go:type': 'tsales:ToflerOrder',
                'tsales:totalAmount>=': 2300,
                'groupBy(tsales:paymentStatus)': None,
                'groupBy(tsales:fulfillmentStatus)': None

            },
            'output': {}
        },
    ]
    twest_config = [
        {
            #filter then max
            'input': {
                'go:type': 'tsales:ToflerOrder',
                'tsales:totalAmount>=': 1000,
                'tsales:toflerProduct': {
                    'max(tsales:price)': None,
                    'tsales:deliveredStatus': "DELIVERED"
                }
            },
            'output': {}
        }
    ]
    groupby_config = [
        # {
        #     #groupby then filter
        #     'input': {
        #         'go:type': 'tsales:ToflerOrder',
        #         'groupBy(paymentMode)': None,
        #         'tsales:buyer': {
        #             'groupBy(tsales:userId)': None
        #         },
        #     },
        #     'output': {}
        # },
        # {
        #     #above with filter at same level but different entities
        #     'input': {
        #         'go:type': 'tsales:ToflerOrder',
        #         'tsales:totalAmount>=': 1000,
        #         'id': None,
        #         'tsales:buyer':  {
        #             'max(tsales:userId)': None,
        #             'id': None,
        #             # 'tsales:userId': 15429,
        #         },
        #         'tsales:toflerProduct': {
        #             'groupBy(tsales:deliveredStatus)': None,
        #             'count(tsales:toflerProductCIN)': None
        #             # 'tsales:deliveredStatus': 'DELIVERED',
        #         },
        #     },
        #     'output': {}
        # },

        #groupby fields tsales:fulfillmentStatus tsales:paymentMode
        # tsales:paymentStatus tsales:toflerProductCIN tsales:deliveredStatus
        # {
        #     'input': {
        #         'groupBy(tsales:paymentStatus)': None,
        #         'go:type': 'tsales:ToflerOrder',
        #         'tsales:totalAmount>=': 10,
        #         'tsales:toflerProduct': {
        #             'tsales:deliveredStatus': "DELIVERED"
        #         }
        #     },
        #     'output': {}
        # }
        # ,{
        #     'input': {
        #
        #         'go:type': 'tsales:ToflerOrder',
        #         'tsales:totalAmount>=': 10,
        #         'tsales:toflerProduct': {
        #               'groupBy(tsales:deliveredStatus)': None
        #         }
        #     },
        #     'output': {}
        # }
        #above 4 cases with relational props
    ]

    not_supported = [
        {
            #count then filter on relational property not supported
            'input': {
                'go:type': 'tsales:ToflerOrder',
                'sum(tsales:totalAmount)': None,
                'tsales:toflerProduct': {
                    'tsales:toflerProductCIN': {
                        'ingovmca:cin': 'L27100MH1907PLC000260'
                    },
                },
            },
            'output': {}
        },
        {
            #min then filter
            'input': {
                'go:type': 'tsales:ToflerOrder',
                'min(tsales:totalAmount)': None,
                'tsales:toflerProduct': {
                    'tsales:toflerProductCIN': {
                        'ingovmca:cin': 'L27100MH1907PLC000260'
                    },
                },
            },
            'output': {}
        },
    ]

    def setUp(self, test_self, config):
        print "%s Inside setup" % (test_self.__class__.__name__)

    def test_method(self, test_self, config):
        AUTH_KEY = 'LUDLFiqRS2g469KmX2YGanihmIQpblwoXbNBjG4mjIdADZZTuJRZodEz80cPHjaN'
        url = 'http://localhost:8888/query'
        response = requests.post(
            url, data=json.dumps(config['input']))
        result = json.loads(response.content)
        print json.dumps(result, indent=4)
        # test_self.assertEquals(result, config['output'])


TestGraphQueryV2().run_test_suite()
