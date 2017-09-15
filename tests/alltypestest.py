import testbase
# import json
from toflerdb.utils import collection
from toflerdb.dbutils import snapshot as snapshot_dbutils


class TestAllTypesInSnapshot(testbase.TestBase):

    def test_all_types(self):
        node_id = 'dcn8bzb83mxftp'
        node_info = snapshot_dbutils.get_snapshot_nodes(id=node_id)
        # print json.dumps(node_info, indent=4)
        should_types = ['gctest:Book', 'to:Entity']
        node_type = node_info[node_id]['to:type'][0]['value']
        self.assertTrue(collection.is_equal_list(node_type, should_types))
        should_types = [
            'to:ComplexProperty',
            'to:Property',
            'gctest:pageNumber'
        ]
        pageNumber_type = node_info[node_id][
            'gctest:pageNumber'][0]['to:type'][0]['value']
        self.assertTrue(collection.is_equal_list(
            pageNumber_type, should_types))


if __name__ == '__main__':
    testbase.runtest()
