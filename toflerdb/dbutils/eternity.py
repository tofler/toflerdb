from toflerdb.utils.common import Common
from toflerdb.utils import templatizedid
from toflerdb.config import FACT_STATUS


def convert_elements_to_string(subj, pred, objc):
    if subj is not None:
        subj = str(subj)
    if pred is not None:
        pred = str(pred)
    if objc is not None:
        objc = str(objc)

    return (subj, pred, objc)


def tuple_exists_in_eternity(subj, pred, objc):
    (subj, pred, objc) = convert_elements_to_string(subj, pred, objc)
    # print type(subj), type(pred), type(objc)
    # print subj, pred, objc
    query = """
        SELECT subject FROM toflerdb_eternity WHERE subject = %s AND predicate
        = %s AND (object = %s OR value = %s) AND status != %s
    """
    response = Common.execute_query(query, (
        subj, pred, objc, objc, FACT_STATUS.DELETED))
    if len(response):
        return True
    return False


def insert_into_eternity(inputs):
    if not inputs:
        return
    query = """
        INSERT INTO toflerdb_eternity(fact_id, subject, predicate, object,
         value, uid, prev) VALUES
    """
    query_data = []
    query_clauses = []
    for item in inputs:
        query_clauses.append('%s, %s, %s, %s, %s, %s, %s')
        query_data += [item['fact_id'], item['subject'],
                       item['predicate'], item['object'], item['value'],
                       item['author'], item['prev']]
    query_clauses_str = '), ('.join(query_clauses)
    query += '(' + query_clauses_str + ')'
    Common.execute_query(query, tuple(query_data))


def get_fact_ids(fact_tuples, author=None):
    query = """
        SELECT fact_id FROM toflerdb_eternity WHERE
    """
    placeholder = []
    query_data = []
    for row in fact_tuples:
        row = list(row)
        subj = row[0]
        if templatizedid.is_templatized_id(subj):
            subj = templatizedid.append_userid(subj, author)
            subj = get_id_by_templatized_id(subj)
        if not subj:
            continue

        placeholder.append('(subject=%s AND predicate=%s AND \
                             (object=%s OR value=%s))')


        (subj, row[1], row[2]) = convert_elements_to_string(subj, row[1], row[2])
        query_data += [subj, row[1], row[2], row[2]]

    if not len(placeholder):
        return []

    placeholder_str = ' OR '.join(placeholder)
    query += placeholder_str
    response = Common.execute_query(query, tuple(query_data))
    retval = []
    for res in response:
        retval.append(res['fact_id'])

    return retval


def get_fact_status(fact_ids):
    query = """
        SELECT fact_id, status FROM toflerdb_eternity WHERE fact_id IN
    """
    placeholder_str = ', '.join(['%s' for _ in fact_ids])
    query = '%s (%s)' % (query, placeholder_str)
    query_data = tuple(fact_ids)
    response = Common.execute_query(query, query_data)
    retval = {res['fact_id']: res['status'] for res in response}

    return retval


def is_all_eternity_facts_deleted(fact_ids):
    statuses = get_fact_status(fact_ids)
    for fid in fact_ids:
        if statuses.get(fid) != FACT_STATUS.DELETED:
            return False

    return True


def is_none_eternity_facts_deleted(fact_ids):
    statuses = get_fact_status(fact_ids)
    for fid in fact_ids:
        if statuses.get(fid) == FACT_STATUS.DELETED:
            return False

    return True


def get_fact_tuples_by_fact_ids(fact_ids):
    if not isinstance(fact_ids, list):
        fact_ids = [fact_ids]

    placeholder_str = ', '.join(['%s' for _ in fact_ids])
    query = """
        SELECT subject, predicate, object, value FROM toflerdb_eternity
        WHERE fact_id IN (%s)
    """ % placeholder_str
    query_data = tuple(fact_ids)
    response = Common.execute_query(query, query_data)
    return [(res['subject'], res['predicate'], res['object'] or res['value'])
            for res in response]


def get_id_by_templatized_id(temp_id):
    query = """
        SELECT subject FROM toflerdb_eternity WHERE predicate = %s AND
        value = %s
    """
    query_data = ('to:templatizedId', temp_id)
    response = Common.execute_query(query, query_data)
    retval = None
    if len(response):
        retval = response[0]['subject']

    return retval


def is_any_type_facts(fact_ids):
    if not isinstance(fact_ids, list):
        fact_ids = [fact_ids]

    placeholder_str = ', '.join(['%s' for _ in fact_ids])
    query = """
        SELECT fact_id FROM toflerdb_eternity WHERE fact_id IN (%s)
        AND predicate = 'to:type'
    """ % placeholder_str
    query_data = tuple(fact_ids)
    response = Common.execute_query(query, query_data)
    return len(response) > 0
