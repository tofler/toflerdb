import unittest
from toflerdb.core import api as gcc_api
from toflerdb.dbutils import snapshot as snapshot_dbutils
import utils as test_utils


def runtest():
    unittest.main()


class TestBase(unittest.TestCase):

    def setUp(self):
        # insert ontology to be tested on
        self.author = 'toflerdb'
        self.ontology = [
            ('gctest:Book', 'to:subClassOf', 'to:Entity'),
            ('gctest:Author', 'to:subClassOf', 'to:Entity'),
            ('gctest:pageNumber', 'to:subClassOf', 'to:ComplexProperty'),
            ('gctest:pageNumber', 'to:domain', 'gctest:Book'),
            ('gctest:pageNumber', 'to:range', 'to:Null'),
            ('gctest:pageNumberStartsAt', 'to:subClassOf', 'to:Property'),
            ('gctest:pageNumberStartsAt', 'to:domain',
                'gctest:pageNumber'),
            ('gctest:pageNumberStartsAt', 'to:range', 'to:Int'),
            ('gctest:pageNumberEndsAt', 'to:subClassOf', 'to:Property'),
            ('gctest:pageNumberEndsAt', 'to:domain',
                'gctest:pageNumber'),
            ('gctest:pageNumberEndsAt', 'to:range', 'to:Int'),
            ('gctest:hasAuthor', 'to:subClassOf', 'to:RelationalProperty'),
            ('gctest:hasAuthor', 'to:domain', 'gctest:Book'),
            ('gctest:hasAuthor', 'to:range', 'gctest:Author'),
            ('gctest:numVolume', 'to:subClassOf', 'to:Property'),
            ('gctest:numVolume', 'to:domain', 'gctest:Book'),
            ('gctest:numVolume', 'to:range', 'to:Int'),
            ('gctest:numVolume', 'to:isUnique', 'True')
        ]
        # insert few facts to be deleted
        self.fact_list = [
            ('dcn8bzb83mxftp', 'to:type', 'gctest:Book'),
            ('dcn8bzb83mxftp', 'to:label', 'Test Book'),
            ('dcn8bzb83mxftp', 'to:description',
                'This book entry is done for testing deletion'),
            ('dcn8bzb83mxftp', 'gctest:numVolume', '4'),
            ('dcn8b17d8zkyr8', 'to:type', 'gctest:Author'),
            ('dcn8b17d8zkyr8', 'to:label', 'Test Author 1'),
            ('dcn8bzb83mxftp', 'gctest:hasAuthor',
                'dcn8b17d8zkyr8'),
            ('dcn8b28shzvk67', 'to:type', 'gctest:Author'),
            ('dcn8b28shzvk67', 'to:label', 'Test Author 2'),
            ('dcn8bzb83mxftp', 'gctest:hasAuthor',
                'dcn8b28shzvk67'),
            ('dcn8b37lhdt3fl', 'to:type', 'gctest:pageNumber'),
            ('dcn8b37lhdt3fl', 'gctest:pageNumberStartsAt', '10'),
            ('dcn8b37lhdt3fl', 'gctest:pageNumberEndsAt', '100'),
            ('dcn8bzb83mxftp', 'dcn8b37lhdt3fl', '')
        ]
        self.existing_nodes = [
            'dcn8bzb83mxftp',
            'dcn8b17d8zkyr8',
            'dcn8b28shzvk67'
        ]

        gcc_api.insert_ontology(self.ontology)
        test_utils.do_sleep(msg='Inserting ontology')
        gcc_api.insert_facts(self.fact_list, author=self.author)
        test_utils.do_sleep(msg='Inserting facts')
        self.fact_ids = test_utils.get_fact_ids(self.fact_list)

    def tearDown(self):
        test_utils.delete_from_eternity(self.fact_ids)
        snapshot_dbutils.delete_snapshot_nodes_by_id(self.existing_nodes)
