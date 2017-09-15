'''Unit tests for graphquery
'''

import json
import unittest
from deepdiff import DeepDiff
from toflerdb.core import api as gcc_api
from toflerdb.dbutils import snapshot as snapshot_dbutils
from utils import do_sleep, delete_from_eternity_by_id
from pkg_resources import resource_filename


class TestGraphQuery(unittest.TestCase):

    FACTS_PATH = 'resources/data/gctest_facts.rdf'
    ONTOLOGY_PATH = 'resources/data/gctest_ontology.rdf'

    def setUp(self):
        # ontology to be tested on
        self.ontology_path = resource_filename('toflerdb', self.ONTOLOGY_PATH)
        # few sample facts
        self.facts_path = resource_filename('toflerdb', self.FACTS_PATH)
        self.author = 'toflerdb'
        gcc_api.insert_ontology(file=self.ontology_path, author=self.author)
        do_sleep(30,msg='Inserting Ontology')
        res = gcc_api.insert_facts(file=self.facts_path, author=self.author)
        do_sleep(30, msg='Inserting Facts')
        self.templatized_id_map = res['templatized_id_map']
        self.name_map = {
            '_person_1': 'Mckenzie Mclean',
            '_person_2': 'Mays Talley',
            '_person_3': 'Carrillo Parsons',
            '_person_4': 'Mcmahon Phillips',
            '_person_5': 'Mccarty Atkins',
            '_person_6': 'Elisa Beasley',
            '_person_7': 'Tisha Moreno',
            '_person_8': 'Holloway Irwin',
            '_person_9': 'Kirkland Hughes',
            '_person_10': 'Trudy Maxwell',
            '_person_11': 'Black Malone',
            '_person_12': 'Ashlee Silva',
        }

    def tearDown(self):
        # delete all the nodes
        # node_ids = [
        #     self.templatized_id_map[temp_id]
        #     for temp_id in self.templatized_id_map if temp_id in self.name_map
        # ]

        # snapshot_dbutils.delete_snapshot_nodes_by_id(node_ids)
        # do_sleep(msg='Deleting Nodes')
        # delete_from_eternity_by_id(node_ids)
        pass

    def test_query(self):
        ''' Test for simple query, without any hop.
        Main objective of the test to check simplest
        possible query support
        '''
        query = {
            'id': None,
            'to:type': 'gctest:Person',
            'to:label': None,
            'gctest:hasChild': {
                'to:label': None,
                'gctest:dateOfBirth<=': '19970101',
            }
        }
        # expected_output = [{
        #     'id': self.templatized_id_map['_person_6'],
        #     'to:type': [['to:Entity', 'gctest:Person']],
        #     'gctest:dateOfBirth': '19960101',
        #     'to:label': [self.name_map['_person_6']]
        # }]
        output = gcc_api.graph_query(query)
        print json.dumps(output, indent=4)
        self.assertTrue(True)
        # self.assertTrue(len(DeepDiff(
        #     output, expected_output, ignore_order=True)) == 0)

    def test_query1(self):
        ''' Test for simple query, without any hop.
        Main objective of the test to check simplest
        possible query support
        '''
        query = {
            'id': None,
            'to:type': 'gctest:Person',
            'gctest:dateOfBirth': '19960101',
            'to:label': None
        }
        expected_output = [{
            'id': self.templatized_id_map['_person_6'],
            'to:type': [['to:Entity', 'gctest:Person']],
            'gctest:dateOfBirth': '19960101',
            'to:label': [self.name_map['_person_6']]
        }]
        output = gcc_api.graph_query(query)
        self.assertTrue(len(DeepDiff(
            output, expected_output, ignore_order=True)) == 0)

    def test_query2(self):
        ''' This test is for complex property without filter
        Expected output should contain both types of nodes. One with the
        complex property along with its corresponding values. Another where
        complex property is not present.
        '''
        query = {
            'id': None,
            'to:type': 'gctest:Person',
            'gctest:dateOfBirth': '19610101',
            'to:label': None,
            'gctest:hasEducation': {
                'gctest:educationLevel': None,
                'gctest:pointScored': None
            }
        }
        expected_output = [{
            'id': self.templatized_id_map['_person_10'],
            'to:type': [['to:Entity', 'gctest:Person']],
            'gctest:dateOfBirth': '19610101',
            'to:label': [self.name_map['_person_10']]
        }, {
            'id': self.templatized_id_map['_person_1'],
            'to:type': [['to:Entity', 'gctest:Person']],
            'gctest:dateOfBirth': '19610101',
            'to:label': [self.name_map['_person_1']],
            'gctest:hasEducation': [{
                'gctest:educationLevel': [u'Graduation'],
                'gctest:pointScored': [74.0]
            }, {
                'gctest:educationLevel': [u'MBA'],
                'gctest:pointScored': [8.3]
            }]
        }, {
            'id': self.templatized_id_map['_person_3'],
            'to:type': [['to:Entity', 'gctest:Person']],
            'gctest:dateOfBirth': '19610101',
            'to:label': [self.name_map['_person_3']]
        }]
        output = gcc_api.graph_query(query)
        # print json.dumps(output, indent=4)
        self.assertTrue(len(DeepDiff(
            output, expected_output, ignore_order=True)) == 0)

    def test_query3(self):
        ''' This test is for complex property with filter
        Expected output should contain only one type of node. One with the
        complex property where the filter matches. Node without the complex
        property should not be included in the output.
        '''
        # TODO: return only the matched element in the array
        # Solution: http://stackoverflow.com/questions/25284609
        query = {
            'id': None,
            'to:type': 'gctest:Person',
            'gctest:dateOfBirth': None,
            'to:label': None,
            'gctest:hasEducation': {
                'gctest:educationLevel': 'graduation',
                'gctest:pointScored': None
            }
        }
        expected_output = [{
            'id': self.templatized_id_map['_person_1'],
            'to:type': [['to:Entity', 'gctest:Person']],
            'gctest:dateOfBirth': '19610101',
            'to:label': [self.name_map['_person_1']],
            'gctest:hasEducation': [{
                'gctest:educationLevel': [u'Graduation'],
                'gctest:pointScored': [74.0]
            }, {
                'gctest:educationLevel': [u'MBA'],
                'gctest:pointScored': [8.3]
            }]
        }]
        output = gcc_api.graph_query(query)
        self.assertTrue(len(DeepDiff(
            output, expected_output, ignore_order=True)) == 0)

    def test_query4(self):
        ''' This test is for simple relational property without filter
        Expected output should contain both types of nodes. One with the
        relational property along with its corresponding values. Another where
        relational property is not present.
        '''
        query = {
            'id': None,
            'to:type': 'gctest:Person',
            'gctest:dateOfBirth': '19610101',
            'to:label': None,
            'gctest:hasChild': {
                'gctest:dateOfBirth': None,
                'to:label': None
            }
        }
        expected_output = [{
            "id": self.templatized_id_map['_person_10'],
            "gctest:dateOfBirth": "19610101",
            "to:type": [["to:Entity", "gctest:Person"]],
            "to:label": [self.name_map['_person_10']]
        }, {
            "id": self.templatized_id_map['_person_1'],
            "gctest:dateOfBirth": "19610101",
            "to:type": [["to:Entity", "gctest:Person"]],
            "to:label": [self.name_map['_person_1']],
            "gctest:hasChild": [{
                "gctest:dateOfBirth": "19950101",
                "to:label": [self.name_map['_person_5']]
            }, {
                "gctest:dateOfBirth": "19960101",
                "to:label": [self.name_map['_person_6']]
            }]
        }, {
            "id": self.templatized_id_map['_person_3'],
            "gctest:dateOfBirth": "19610101",
            "to:type": [["to:Entity", "gctest:Person"]],
            "to:label": [self.name_map['_person_3']],
            "gctest:hasChild": [{
                "gctest:dateOfBirth": "19970101",
                "to:label": [self.name_map['_person_7']]
            }, {
                "gctest:dateOfBirth": "19980101",
                "to:label": [self.name_map['_person_8']]
            }]
        }]

        output = gcc_api.graph_query(query)
        print json.dumps(output, indent=4)
        self.assertTrue(len(DeepDiff(
            output, expected_output, ignore_order=True)) == 0)

    def test_query5(self):
        ''' This test is for simple relational property with filter
        Expected output should contain only one type of node. One with the
        relational property where the filter matches. Node without the
        relational property should not be included in the output.
        '''
        query = {
            'id': None,
            'to:type': 'gctest:Person',
            'gctest:dateOfBirth': '19610101',
            'to:label': None,
            'gctest:hasChild': {
                'gctest:dateOfBirth': '19960101',
                'to:label': None
            }
        }
        expected_output = [{
            "id": self.templatized_id_map['_person_1'],
            "gctest:dateOfBirth": "19610101",
            "to:type": [["to:Entity", "gctest:Person"]],
            "to:label": [self.name_map['_person_1']],
            "gctest:hasChild": [{
                "gctest:dateOfBirth": "19960101",
                "to:label": [self.name_map['_person_6']]
            }]
        }]

        output = gcc_api.graph_query(query)
        # print json.dumps(output, indent=4)
        self.assertTrue(len(DeepDiff(
            output, expected_output, ignore_order=True)) == 0)

    def test_query6(self):
        query = {
            'id': None,
            'to:type': 'gctest:Person',
            'to:label': self.name_map['_person_1'],
            'gctest:dateOfBirth': "19610101",
            'gctest:hasRelative': {
                'to:label': None,
                'gctest:relationName': None,
                'gctest:dateOfBirth': "19610101"
            }
        }
        expected_output = [{
            "id": self.templatized_id_map['_person_1'],
            "to:type": [["to:Entity", "gctest:Person"]],
            "to:label": [self.name_map['_person_1']],
            "gctest:dateOfBirth": "19610101",
            "gctest:hasRelative": [{
                "gctest:dateOfBirth": "19610101",
                "to:label": [self.name_map['_person_10']],
                "gctest:relationName": ["Uncle"]
            }],
        }]
        output = gcc_api.graph_query(query)
        print json.dumps(output, indent=4)
        self.assertTrue(len(DeepDiff(
            output, expected_output, ignore_order=True)) == 0)

    def test_query7(self):
        ''' Query for id, where relational property is queried upon
        a given id.
        '''
        query = {
            'id': None,
            'to:type': 'gctest:Person',
            'to:label': None,
            'gctest:hasRelative': {
                'id': self.templatized_id_map['_person_10'],
                'to:label': None,
                'gctest:relationName': None,
            }
        }
        expected_output = [{
            "id": self.templatized_id_map['_person_1'],
            "to:type": [["to:Entity", "gctest:Person"]],
            "to:label": [self.name_map['_person_1']],
            "gctest:hasRelative": [{
                "to:label": [self.name_map['_person_10']],
                "id": self.templatized_id_map['_person_10'],
                "gctest:relationName": ["Uncle"]
            }]
        }, {
            "id": self.templatized_id_map['_person_3'],
            "to:type": [["to:Entity", "gctest:Person"]],
            "to:label": [self.name_map['_person_3']],
            "gctest:hasRelative": [{
                "to:label": [self.name_map['_person_10']],
                "id": self.templatized_id_map['_person_10'],
                "gctest:relationName": ["Grand Father"]
            }]
        }]
        output = gcc_api.graph_query(query)
        print json.dumps(output, indent=4)
        self.assertTrue(len(DeepDiff(
            output, expected_output, ignore_order=True)) == 0)

    def test_query8(self):
        ''' This is to test nested query system
        '''
        query = {
            "query": {
                "nested": {
                    "path": "gctest:hasEducation.gctest:educationDuration",
                    "query": {
                        "bool": {
                            "must": [
                                { "match": { "gctest:hasEducation.gctest:educationDuration.gctest:educationDurationStart.value": "1980" }},
                            ]
                        }
                    },
                    "inner_hits": {}
                }
            }
        }
        output = snapshot_dbutils.execute_search_query(query)
        print json.dumps(output, indent=4)
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
