'''ElasticSearch easy-access module

This module makes access to an ElasticSearch database very simple.
'''

from elasticsearch import Elasticsearch, helpers


class Elastic(object):

    def __init__(self, host, port):
        self._config = {
            'host': host,
            'port': port,
        }
        self._elasticdb = Elasticsearch(
            [{
                'host': self._config['host'],
                'port': self._config['port']
            }]
        )

    def get_connection(self):
        return self._elasticdb

    def put_bulk_data(self, index, mapping_name=None, data=[]):
        es = self.get_connection()
        actions = []
        response = None
        get_mapping_from_data = False
        if mapping_name is None:
            get_mapping_from_data = True
        for item in data:
            if get_mapping_from_data:
                action = {
                    '_index': index,
                    '_type': item[1],
                    '_source': item[0]
                }
                if 'id' in item[0]:
                    action['_id'] = item[0]['id']
            else:
                action = {
                    '_index': index,
                    '_type': mapping_name,
                    '_source': item
                }
                if 'id' in item:
                    action['_id'] = item['id']
            actions.append(action)
        if len(actions):
            response = helpers.bulk(es, actions)
        return response
