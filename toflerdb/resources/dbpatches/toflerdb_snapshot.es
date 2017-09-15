{
    "settings": {
        "number_of_shards": 10,
        "analysis": {
            "char_filter": {
                "index_delimiter": {
                    "type": "mapping",
                    "mappings": [",=>", ".=>\\u0020", "-=>\\u0020"]
                },
                "search_delimiter": {
                    "type": "mapping",
                    "mappings": [",=>", ".=>\\u0020", "-=>\\u0020"]
                },
                "space_pattern": {
                    "type": "pattern_replace",
                    "pattern": "\\s+",
                    "replacement": " "
                }
            },
            "analyzer": {
                "simple_keyword": {
                    "type": "custom",
                    "tokenizer": "keyword",
                    "filter": ["lowercase"],
                    "char_filter": ["index_delimiter"]
                },
                "simple_edgegram": {
                    "type": "custom",
                    "tokenizer": "simple_edgegram",
                    "filter": ["lowercase"],
                    "char_filter": ["index_delimiter"]
                },
                "index_whitespace_edgegram": {
                    "type": "custom",
                    "tokenizer": "whitespace_edgegram",
                    "filter": ["lowercase"],
                    "char_filter": ["index_delimiter"]
                },
                "index_whitespace_keyword": {
                    "type": "custom",
                    "tokenizer": "whitespace",
                    "filter": ["lowercase"],
                    "char_filter": ["index_delimiter"]
                },
                "search_whitespace_keyword": {
                    "type": "custom",
                    "tokenizer": "whitespace",
                    "filter": ["lowercase"],
                    "char_filter": ["search_delimiter", "space_pattern"]
                }
            },
            "tokenizer": {
                "simple_edgegram": {
                    "type": "edgeNGram",
                    "min_gram": 1,
                    "max_gram": 100,
                    "token_chars": ["letter", "digit", "symbol", "punctuation", "whitespace"]
                },
                "whitespace_edgegram": {
                    "type": "edgeNGram",
                    "min_gram": 1,
                    "max_gram": 20,
                    "token_chars": ["letter", "digit", "symbol", "punctuation"]
                }
            }
        }
    }
}
