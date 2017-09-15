{
    "properties": {
        "id": {"type": "string", "index": "not_analyzed"},
        "go:type": {
            "properties": {
                "fact_id": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "value": {
                    "index": "not_analyzed",
                    "type": "string"
                }
            }
        },
        "go:templatizedId": {
            "properties": {
                "fact_id": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "value": {
                    "index": "not_analyzed",
                    "type": "string"
                }
            }
        },
        "go:label": {
            "properties": {
                "fact_id": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "value": {
                    "type": "string",
                    "index": "not_analyzed",
                    "fields": {
                        "value": {
                            "type": "string",
                            "analyzer": "simple_edgegram",
                            "search_analyzer": "simple_keyword"
                        },
                        "whitespace_edgegram": {
                            "type": "string",
                            "analyzer": "index_whitespace_edgegram",
                            "search_analyzer": "search_whitespace_keyword"
                        },
                        "whitespace_keyword": {
                            "type": "string",
                            "analyzer": "index_whitespace_keyword",
                            "search_analyzer": "search_whitespace_keyword"
                        }
                    }
                }
            }
        }
    }
}
