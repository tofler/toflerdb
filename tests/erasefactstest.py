'''Unit tests for the Deleting facts into toflerdb

'''

import unittest
from toflerdb.utils.common import Common
from toflerdb.core.entry_maker import EntryMaker
from toflerdb.core.onto_maker import OntoMaker
from toflerdb.core.eraser import Eraser
from toflerdb.dbutils import snapshot as snapshot_dbutils
from toflerdb.dbutils import eternity as eternity_dbutils
from toflerdb.utils import collection
import utils as test_utils


def insert_ontology(onto_tuples):
    om = OntoMaker()
    om.turn_off_validation()
    for row in onto_tuples:
        om.add_input(row[0], row[1], row[2])

    om.make_mapping()
    om.commit()
    test_utils.do_sleep(2, msg='Creating Ontology')


def insert_facts(fact_tuples):
    em = EntryMaker()
    em.turn_off_validation()
    for row in fact_tuples:
        em.add_input(row[0], row[1], row[2])
    em.commit()
    test_utils.do_sleep(2, msg='Inserting')


def get_fact_ids(fact_tuples, filter_idx=[]):
    tpl = [fact_tuples[i] for i in filter_idx] if filter_idx else fact_tuples
    return eternity_dbutils.get_fact_ids(tpl)


def delete_facts(fact_ids):
    ers = Eraser()
    ers.erase_facts(fact_ids)
    test_utils.do_sleep(3, msg='Deleting')


def delete_node(node_ids):
    ers = Eraser()
    ers.erase_nodes(node_ids)
    test_utils.do_sleep(2, msg='Deleting Node')


def delete_from_eternity(fact_ids):
    query = """
        DELETE FROM toflerdb_eternity WHERE fact_id IN
    """
    placeholder = ', '.join(['%s' for _ in fact_ids])
    query = '%s (%s)' % (query, placeholder)
    query_data = tuple(fact_ids)

    Common.execute_query(query, query_data)


class TestEraseFacts(unittest.TestCase):

    def setUp(self):
        # insert ontology to be tested on
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
            ('gctest:numVolume', 'to:range', 'to:Interger'),
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
            ('dcn8b37lhdt3fl', 'gctest:pageNumberStartsAt', 10),
            ('dcn8b37lhdt3fl', 'gctest:pageNumberEndsAt', 100),
            ('dcn8bzb83mxftp', 'dcn8b37lhdt3fl', '')
        ]
        self.existing_nodes = [
            'dcn8bzb83mxftp',
            'dcn8b17d8zkyr8',
            'dcn8b28shzvk67'
        ]
        # index of different kinds of facts which needs to be deleted
        self.simple_unique_facts = [3]
        self.simple_multi_entry_facts = [9]
        self.complex_facts = [13]

        insert_ontology(self.ontology)
        insert_facts(self.fact_list)
        self.fact_ids = get_fact_ids(self.fact_list)
        # delete facts
        # tests to be done
        # delete fact at simple property label
        # delete fact at complex property label
        # delete fact for is_unique property value
        # delete one fact from multi valued property
        # delete the only fact from multi valued property

        # delete nodes
        # test to be done
        # delete independent node
        # delete relational property node: this should delete all
        # the incoming property node

    def tearDown(self):
        delete_from_eternity(self.fact_ids)
        snapshot_dbutils.delete_snapshot_nodes_by_id(self.existing_nodes)

    def test_delete_simple_unique_fact(self):
        to_delete_facts = get_fact_ids(self.fact_list,
                                       self.simple_unique_facts)
        not_deleted_facts = collection.subtract(self.fact_ids, to_delete_facts)
        delete_facts(to_delete_facts)
        response = snapshot_dbutils.get_snapshot_nodes(
            fact_id=to_delete_facts)
        self.assertIsNone(response)
        self.assertIsNotNone(snapshot_dbutils.get_snapshot_nodes(
            fact_id=not_deleted_facts))

    def test_delete_simple_multi_entry_fact(self):
        to_delete_facts = get_fact_ids(self.fact_list,
                                       self.simple_multi_entry_facts)
        should_present_facts = get_fact_ids(self.fact_list, [6])
        delete_facts(to_delete_facts)
        self.assertIsNone(snapshot_dbutils.get_snapshot_nodes(
            fact_id=to_delete_facts))
        self.assertIsNotNone(snapshot_dbutils.get_snapshot_nodes(
            fact_id=should_present_facts))

    def test_delete_complex_fact(self):
        to_delete_facts = get_fact_ids(self.fact_list,
                                       self.complex_facts)
        should_delete_facts = get_fact_ids(self.fact_list, [10, 11, 12])

        not_deleted_facts = collection.subtract(
            self.fact_ids, collection.merge_unique(
                to_delete_facts, should_delete_facts))
        delete_facts(to_delete_facts)
        self.assertIsNone(snapshot_dbutils.get_snapshot_nodes(
            fact_id=to_delete_facts))
        self.assertIsNone(snapshot_dbutils.get_snapshot_nodes(
            fact_id=should_delete_facts))
        self.assertIsNotNone(snapshot_dbutils.get_snapshot_nodes(
            fact_id=not_deleted_facts))
        self.assertTrue(eternity_dbutils.is_all_eternity_facts_deleted(
            collection.merge_unique(to_delete_facts, should_delete_facts)))
        self.assertTrue(eternity_dbutils.is_none_eternity_facts_deleted(
            not_deleted_facts))

    # def test_delete_independent_node(self):
    #     pass

    def test_delete_dependent_node(self):
        to_delete_node = 'dcn8b17d8zkyr8'
        self.existing_nodes.remove(to_delete_node)
        delete_node(to_delete_node)
        should_delete_facts = get_fact_ids(self.fact_list, [4, 5, 6])
        not_deleted_facts = collection.subtract(
            self.fact_ids, should_delete_facts)
        self.assertIsNone(snapshot_dbutils.get_snapshot_nodes(
            fact_id=should_delete_facts))
        self.assertIsNotNone(snapshot_dbutils.get_snapshot_nodes(
            fact_id=not_deleted_facts))
        self.assertIsNone(snapshot_dbutils.get_snapshot_nodes(
            id=to_delete_node))


if __name__ == '__main__':
    unittest.main()
