# import json
import datetime
from toflerdb.utils.common import Common
from toflerdb.utils import collection
from toflerdb.config import FACT_STATUS
import snapshot as snapshot_dbutils


def erase_facts_from_eternity(fact_ids, author=None):
    if not isinstance(fact_ids, list):
        fact_ids = [fact_ids]

    query = """
        UPDATE toflerdb_eternity SET status = %s, status_updated_on = %s,
        status_updated_by = %s WHERE fact_id IN
    """
    placeholder_str = ', '.join(['%s' for _ in fact_ids])
    query = '%s (%s)' % (query, placeholder_str)
    query_data = tuple([FACT_STATUS.DELETED, datetime.datetime.now(), author] +
                       fact_ids)
    Common.execute_query(query, query_data)


def is_empty_node(node):
    if node is None:
        return True
    if len(node.keys()) == 1 and node.keys()[0] == 'id':
        return True

    return False


def erase_facts_from_snapshot(fact_ids):
    if not isinstance(fact_ids, list):
        fact_ids = [fact_ids]

    # get all the entities where factid matches
    # for each entity delete specific segment of the object
    # reinsert all the entities into elastic

    matched_entities = snapshot_dbutils.get_snapshot_nodes(fact_id=fact_ids)
    processed_entities = None
    missing_entities = None
    if matched_entities:
        # print json.dumps(matched_entities, indent=4)
        processed_entities = collection.delete_segment(
            matched_entities, list(map(lambda fid: {'fact_id': fid},
                                       fact_ids)))
        # print json.dumps(processed_entities, indent=4)
    if processed_entities:
        snapshot_dbutils.insert_into_snapshot(processed_entities)
        missing_entities = [entity_id for entity_id in matched_entities
                            if is_empty_node(
                                processed_entities.get(entity_id))]

    if missing_entities:
        snapshot_dbutils.delete_snapshot_nodes_by_id(missing_entities)


def find_subfact_ids(fact_ids):
    if not isinstance(fact_ids, list):
        fact_ids = [fact_ids]

    query = """
        SELECT predicate FROM toflerdb_eternity WHERE fact_id IN
    """
    placeholder_str = ', '.join(['%s' for _ in fact_ids])
    query = '%s (%s)' % (query, placeholder_str)
    query_data = tuple(fact_ids)
    response = Common.execute_query(query, query_data)
    pred = [res['predicate'] for res in response]
    query = """
        SELECT fact_id FROM toflerdb_eternity WHERE subject IN
    """
    placeholder_str = ', '.join(['%s' for _ in pred])
    query = '%s (%s)' % (query, placeholder_str)
    query_data = tuple(pred)
    response = Common.execute_query(query, query_data)
    retval = [res['fact_id'] for res in response]

    return retval


def find_related_facts_by_node_id(node_ids):
    if not isinstance(node_ids, list):
        node_ids = [node_ids]

    placeholder_str = ', '.join(['%s' for _ in node_ids])
    query = """
        SELECT fact_id FROM toflerdb_eternity WHERE subject IN (%s)
        OR object IN (%s)
    """ % (placeholder_str, placeholder_str)
    query_data = tuple(node_ids + node_ids)
    response = Common.execute_query(query, query_data)

    return [res['fact_id'] for res in response]
