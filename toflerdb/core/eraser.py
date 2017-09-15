from toflerdb.dbutils import eraser as eraser_dbutils
from toflerdb.utils import collection


class Eraser(object):

    def __init__(self, **kwargs):
        self._author = kwargs.get('author')

    def populate_subfacts(self, fact_ids):
        subfact_ids = eraser_dbutils.find_subfact_ids(fact_ids)
        return collection.merge_unique(fact_ids, subfact_ids)

    def erase_facts(self, fact_ids):
        # type deletion must be strictly restricted
        if not isinstance(fact_ids, list):
            fact_ids = [fact_ids]
        # we need to delete all the associated facts about any complex
        # ( realtional) fact from eternity also.
        # if the given fact id is a fact id of a complex (realtional/)property
        # add all the sub fact ids of that node to the deletion list
        fact_ids = self.populate_subfacts(fact_ids)
        eraser_dbutils.erase_facts_from_eternity(fact_ids, author=self._author)
        eraser_dbutils.erase_facts_from_snapshot(fact_ids)

    def erase_nodes(self, node_ids):
        if not isinstance(node_ids, list):
            node_ids = [node_ids]

        fact_ids = eraser_dbutils.find_related_facts_by_node_id(node_ids)
        self.erase_facts(fact_ids)
