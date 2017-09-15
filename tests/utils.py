import sys
import time
from toflerdb.utils.common import Common
from toflerdb.dbutils import eternity as eternity_dbutils


def delete_from_eternity(fact_ids):
    query = """
        DELETE FROM toflerdb_eternity WHERE fact_id IN
    """
    placeholder = ', '.join(['%s' for _ in fact_ids])
    query = '%s (%s)' % (query, placeholder)
    query_data = tuple(fact_ids)

    Common.execute_query(query, query_data)


def delete_from_eternity_by_id(ids):
    query = """
        DELETE FROM toflerdb_eternity WHERE subject IN
    """
    placeholder = ', '.join(['%s' for _ in ids])
    query = '%s (%s)' % (query, placeholder)
    query_data = tuple(ids)

    Common.execute_query(query, query_data)


def get_fact_ids(fact_tuples, filter_idx=[]):
    tpl = [fact_tuples[i] for i in filter_idx] if filter_idx else fact_tuples
    return eternity_dbutils.get_fact_ids(tpl)


def do_sleep(count=1, msg='Sleeping'):
    print '\n%s' % msg,
    for _ in range(0, count):
        print '.',
        sys.stdout.flush()
        time.sleep(1)
    print '>>Done.'
