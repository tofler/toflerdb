import os
from pkg_resources import resource_filename
import argparse
from toflerdb.core import api


class Sampler(object):

    DATA_PATH = 'resources/data/'

    def __init__(self):
        self.DATA_PATH = resource_filename('toflerdb', self.DATA_PATH)
        self.ONTOLOGY_FILE = os.path.join(
            self.DATA_PATH, 'ussec.ontology.rdf')
        self.FACTS_FILE = os.path.join(
            self.DATA_PATH, 'ussec.sample-facts.rdf')

    def create_ontology(self):
        print "Creating Ontology ..."
        api.insert_ontology(file=self.ONTOLOGY_FILE, validation=False)
        print "Done ."

    def create_facts(self):
        print "Inserting Facts ..."
        api.insert_facts(
            file=self.FACTS_FILE, author='toflerdb', show_progress=True)
        print "Done ."

    def run(self):
        self.create_ontology()
        self.create_facts()


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='Config file')
    return parser.parse_args()


def run_sample():
    args = parse_arguments()
    prev_conf = ''
    if args.config:
        prev_conf = os.getenv('TOFLERDB_CONF', '')
        os.environ['TOFLERDB_CONF'] = args.config

    sampler = Sampler()
    sampler.run()
    os.environ['TOFLERDB_CONF'] = prev_conf


if __name__ == '__main__':
    run_sample()
