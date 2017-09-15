import json
import requests
import testbase
from toflerdb.dbutils import snapshot as snapshot_dbutils
import utils as test_utils


AUTH_KEY = 'LUDLFiqRS2g469KmX2YGanihmIQpblwoXbNBjG4mjIdADZZTuJRZodEz80cPHjaN'


def is_all_of_type(target_type, response):
    for item in response:
        all_types = item['to:type']
        for t in all_types:
            if target_type not in t:
                return False

    return True


class TestApiServer(testbase.TestBase):

    def test_query_endpoint(self):
        onto_type = 'gctest:Book'
        query = {
            "id": None,
            "to:type": onto_type,
            "to:label": None
        }
        url = 'http://localhost:9100/query'
        response = requests.post(
            url, data=json.dumps(query), timeout=2)
        res_data = json.loads(response.content)
        self.assertTrue(res_data['status'] == 'SUCCESS')
        self.assertTrue(is_all_of_type(onto_type, res_data['content']))

    def test_uploadfacts_endpoint(self):
        url = 'http://localhost:9100/upload/facts'
        query = {
            'fact_tuples': self.fact_list,
            'author': 'toflerdb'
        }
        # as we are going to insert same facts again we are
        # going to delete them
        test_utils.delete_from_eternity(self.fact_ids)
        snapshot_dbutils.delete_snapshot_nodes_by_id(self.existing_nodes)
        response = requests.post(
            url, data=json.dumps(query), timeout=2)
        test_utils.do_sleep(msg='Uploading')
        res_data = json.loads(response.content)
        self.assertTrue(res_data['status'] == 'SUCCESS')
        for node_id in self.existing_nodes:
            self.assertIsNotNone(snapshot_dbutils.get_snapshot_nodes(
                id=node_id))


if __name__ == '__main__':
    testbase.runtest()
