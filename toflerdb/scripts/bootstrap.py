'''
Steps to be followed
to setup the toflerdb project from scratch
OR
to clean up the toflerdb project to its empty state
    Step1: Delete toflerdb_snapshot index if exists
    Step2: Create intial mapping/analyzer for type and label
    Step3: Empty toflerdb_eternity & toflerdb_ontology table
    Step4: Import the common ontology, like Property, Entity
'''

import os
from toflerdb.utils.common import Common
from toflerdb import config
from toflerdb.core import api
from pkg_resources import resource_filename


class Bootstrapper(object):

    SCHEMA_PATH = 'resources/dbpatches/'
    ONTOLOGY_PATH = 'resources/data/to.common.ontology.rdf'

    def __init__(self):
        self.ONTOLOGY_PATH = resource_filename('toflerdb', self.ONTOLOGY_PATH)
        self.SCHEMA_PATH = resource_filename('toflerdb', self.SCHEMA_PATH)
        self.WORKER_FILE = os.path.join(
            self.SCHEMA_PATH, 'toflerdb_worker.sql')
        self.ONTOLOGY_FILE = os.path.join(
            self.SCHEMA_PATH, 'toflerdb_ontology.sql')
        self.ETERNITY_FILE = os.path.join(
            self.SCHEMA_PATH, 'toflerdb_eternity.sql')
        self.INDEX_MAPPING_FILE = os.path.join(
            self.SCHEMA_PATH, 'toflerdb_snapshot.es')
        self.DOCTYPE_MAPPING_FILE = os.path.join(
            self.SCHEMA_PATH, 'toflerdb_snapshot.node.es')

    def log(self, msg):
        print msg

    def execute_multi_query(self, query):
        queries = query.split(';')
        for q in queries:
            q = q.strip()
            if q:
                Common.execute_query(q)

    def create_sql_tables(self):
        query = open(self.WORKER_FILE, 'r').read()
        self.execute_multi_query(query)
        self.log("Creating Ontology Table ...")
        query = open(self.ONTOLOGY_FILE, 'r').read()
        self.execute_multi_query(query)
        self.log("Creating Eternity Table ...")
        query = open(self.ETERNITY_FILE, 'r').read()
        self.execute_multi_query(query)

    def create_es_mappings(self):
        self.log("Creating Index Mapping ...")
        es = Common.get_elasticsearch_connection()
        es.indices.delete(
            index=config.SNAPSHOT_INDEX, ignore=[400, 404])
        mapping = open(self.INDEX_MAPPING_FILE).read()
        es.indices.create(
            index=config.SNAPSHOT_INDEX, ignore=400, body=mapping)
        mapping = open(self.DOCTYPE_MAPPING_FILE).read()
        es.indices.put_mapping(
            index=config.SNAPSHOT_INDEX,
            doc_type=config.SNAPSHOT_DOC_TYPE,
            ignore=400, body=mapping)

    def reboot(self):
        self.create_es_mappings()
        self.create_sql_tables()
        self.log("Inserting Base Ontology ...")
        prefilled_ontology = [
            ('to:type', 'to:subClassOf', 'to:Property'),
            ('to:type', 'to:domain', 'to:Entity'),
            ('to:type', 'to:domain', 'to:ComplexProperty'),
            ('to:type', 'to:domain', 'to:ComplexRelationalProperty'),
            ('to:type', 'to:range', 'to:Token'),
            ('to:templatizedId', 'to:subClassOf', 'to:Property'),
            ('to:templatizedId', 'to:domain', 'to:Entity'),
            ('to:templatizedId', 'to:domain', 'to:ComplexProperty'),
            ('to:templatizedId', 'to:domain', 'to:ComplexRelationalProperty'),
            ('to:templatizedId', 'to:range', 'to:ID'),
            ('to:templatizedId', 'to:isUnique', 'True'),
            ('to:label', 'to:subClassOf', 'to:Property'),
            ('to:label', 'to:domain', 'to:Entity'),
            ('to:label', 'to:range', 'to:String')
        ]
        query = """
            INSERT INTO toflerdb_ontology(subject, predicate, value)
            VALUES(%s, %s, %s)
        """
        for row in prefilled_ontology:
            Common.execute_query(query, row)

        api.insert_ontology(
            file=self.ONTOLOGY_PATH,
            validation=False)


def run_bootstrap():
    bootstrapper = Bootstrapper()
    bootstrapper.reboot()


if __name__ == '__main__':
    run_bootstrap()
