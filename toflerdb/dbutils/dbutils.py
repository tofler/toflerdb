from toflerdb.utils.common import Common
from toflerdb.utils import collection
from toflerdb.config import SNAPSHOT_INDEX, SNAPSHOT_DOC_TYPE


def insert_entity(entity_id, entity_body):
    es = Common.get_elasticsearch_connection()
    es.index(SNAPSHOT_INDEX, 'entity', body=entity_body, id=entity_id)


def get_root_type(element):
    pass


def get_type(element):
    query = """
        SELECT * FROM toflerdb_ontology WHERE subject = %s AND predicate = %s
    """
    query_data = (element, 'rdfs:type')
    response = Common.execute_query(query, query_data)
    if len(response):
        return response[0]['predicate']
    return None


def get_direct_predicate_value(subj, pred, additional_lookup={}):
    if not subj:
        return []
    if not type(subj) == list:
        subj = [subj]
    or_clauses = ' OR '.join(['subject = %s' for _ in subj])
    query = """
        SELECT subject, predicate, object, value FROM toflerdb_eternity
        WHERE predicate = %s AND
    """
    query += '(' + or_clauses + ')'
    query_data = tuple([pred] + subj)
    # print query
    response = Common.execute_query(query, query_data)
    if len(response) == 0:
        query = """
            SELECT subject, predicate, object, value FROM toflerdb_ontology
            WHERE predicate = %s AND
        """
        query += '(' + or_clauses + ')'
        response = Common.execute_query(query, query_data)

    new_values = []
    for res in response:
        val = res['object']
        if val is None:
            val = res['value']
        new_values.append(val)

    for s in subj:
        if s in additional_lookup and pred in additional_lookup[s]:
            new_values += [x for x in additional_lookup[s]
                           [pred] if x not in new_values]
    return new_values


def get_predicate_value(subj, pred, level=10000, additional_lookup={}):
    # if level >= 0:
    #     root = True
    # if pred == 'rdf:type':
    level -= 1
    new_values = get_direct_predicate_value(subj, pred, additional_lookup)
    if len(new_values) == 0 or level <= 0:
        return new_values

    for item in new_values:
        values = get_predicate_value(item, pred, level=level,
                                     additional_lookup=additional_lookup)
        new_values = collection.merge_unique(new_values, values)
    return new_values


def get_direct_inverse_predicate_value(objc, pred, additional_lookup={}):
    if not objc:
        return []
    if not type(objc) == list:
        objc = [objc]
    or_clauses = ' OR '.join(['object = %s OR value = %s' for _ in objc])
    query = """
        SELECT subject, predicate, object, value FROM toflerdb_eternity
        WHERE predicate = %s AND
    """
    query += '(' + or_clauses + ')'
    objc_data = []
    for x in objc:
        objc_data += [x, x]
    query_data = tuple([pred] + objc_data)
    # print query
    response = Common.execute_query(query, query_data)
    if len(response) == 0:
        query = """
            SELECT subject, predicate, object, value FROM toflerdb_ontology
            WHERE predicate = %s AND
        """
        query += '(' + or_clauses + ')'
        response = Common.execute_query(query, query_data)

    new_values = []
    for res in response:
        val = res['subject']
        new_values.append(val)

    for s in objc:
        if s in additional_lookup and pred in additional_lookup[s]:
            new_values += [x for x in additional_lookup[s]
                           [pred] if x not in new_values]
    return new_values


def get_inverse_predicate_value(objc, pred, level=10000, additional_lookup={}):
    level -= 1
    new_values = get_direct_inverse_predicate_value(
        objc, pred, additional_lookup)
    if len(new_values) == 0 or level <= 0:
        return new_values

    for item in new_values:
        values = get_inverse_predicate_value(
            item, pred, level=level, additional_lookup=additional_lookup)
        new_values = collection.merge_unique(new_values, values)
    return new_values


def exists_in_eternity(subj, additional_lookup={}, eternity_only=False,
                       ontology_only=False):
    response = []
    if not ontology_only:
        query = """
            SELECT subject FROM toflerdb_eternity WHERE subject = %s
        """
        response = Common.execute_query(query, subj)
    if len(response) == 0 and not eternity_only:
        query = """
            SELECT subject FROM toflerdb_ontology WHERE subject = %s
        """
        response = Common.execute_query(query, subj)
    return len(response) > 0 or subj in additional_lookup


def exists_in_ontology(subj, pred, objc):
    query = """
        SELECT subject FROM toflerdb_ontology WHERE subject = %s AND predicate
        = %s AND (object = %s OR value = %s)
    """
    response = Common.execute_query(query, (subj, pred, objc, objc))
    if len(response):
        return True
    return False


def is_unique_predicate_value(pred, additional_lookup={}):
    pred_value = get_predicate_value(
        pred, 'to:isUnique', additional_lookup=additional_lookup)
    is_unique = False
    if len(pred_value) == 1 and pred_value[0]:
        is_unique = True

    return is_unique


def insert_into_ontology(inputs):
    if not inputs:
        return
    query = """
        INSERT INTO toflerdb_ontology(subject, predicate, object, value)
        VALUES
    """
    query_data = []
    query_clauses = []
    for item in inputs:
        query_clauses.append('%s, %s, %s, %s')
        query_data += [item['subject'], item['predicate'],
                       item['object'], item['value']]
    query_clauses_str = '), ('.join(query_clauses)
    query += '(' + query_clauses_str + ')'
    Common.execute_query(query, tuple(query_data))


def get_complete_snapshot_mapping():
    es = Common.get_elasticsearch_connection()
    complete_mapping = es.indices.get_mapping(
        index=SNAPSHOT_INDEX, doc_type=SNAPSHOT_DOC_TYPE)

    return complete_mapping[SNAPSHOT_INDEX]['mappings'][SNAPSHOT_DOC_TYPE]['properties']


def create_snapshot_mapping(mapping):
    es = Common.get_elasticsearch_connection()
    es.indices.put_mapping(index=SNAPSHOT_INDEX,
                           doc_type=SNAPSHOT_DOC_TYPE, body=mapping)


############################################
#          ONTOLOGY ONLY UTILS             #
############################################
def get_all_namespaces():
    query = """
        SELECT DISTINCT(subject) AS subject FROM toflerdb_ontology
    """
    response = Common.execute_query(query)
    all_namespaces = []
    for res in response:
        namespace = res['subject'].split(':')[0]
        if namespace not in all_namespaces:
            all_namespaces.append(namespace)

    return all_namespaces


def get_all_entities_of_namespace(nss):
    if not isinstance(nss, list):
        nss = [nss]
    all_namespaces = get_all_namespaces()
    namespaces = collection.intersection(nss, all_namespaces)
    if not len(namespaces):
        # this is required as the namespace is not a query_data in the
        # following query
        return []
    rx = '|'.join(['%s:' % x for x in namespaces])
    rx = '^(%s).*' % rx
    query = """
        SELECT DISTINCT(subject) AS subject FROM toflerdb_ontology WHERE
        subject REGEXP %s
    """
    response = Common.execute_query(query, rx)
    all_entities = []
    for res in response:
        subj = res['subject']
        subj_subclass = get_predicate_value(subj, 'to:subClassOf')
        if 'to:Entity' in subj_subclass:
            all_entities.append(subj)

    return all_entities


def get_all_properties_where_entity_is_domain_of(entity):
    entity_subclass = get_predicate_value(entity, 'to:subClassOf')
    if not isinstance(entity, list):
        entity = [entity]
    return get_inverse_predicate_value(entity_subclass + entity, 'to:domain')


def get_all_properties_where_entity_is_range_of(entity):
    entity_subclass = get_predicate_value(entity, 'to:subClassOf')
    return get_inverse_predicate_value(entity_subclass + [entity], 'to:range')


def ontology_tree():
    pass


def get_all_propeties(element):
    if not isinstance(element, list):
        element = [element]

    element_subclass = get_predicate_value(element, 'to:subClassOf')
    prop_list = []
    if 'to:RelationalProperty' in element_subclass:
        element_range = get_predicate_value(element, 'to:range')
        element_range_subclass = get_predicate_value(
            element_range, 'to:subClassOf')
        prop_list = get_inverse_predicate_value(
            element_range + element_range_subclass, 'to:domain')
    elif 'to:ComplexRelationalProperty' in element_subclass:
        # if not isinstance(element, list):
        #     element = [element]
        element_range = get_predicate_value(element, 'to:range')
        element_range_subclass = get_predicate_value(
            element_range, 'to:subClassOf')
        element_subclass = get_predicate_value(element, 'to:subClassOf')
        prop_list = get_inverse_predicate_value(
            element_subclass + element + element_range + element_range_subclass, 'to:domain')
    elif 'to:ComplexProperty' in element_subclass or 'to:Entity' in element_subclass:
        # if not isinstance(element, list):
        #     element = [element]
        # element_subclass = get_predicate_value(element, 'to:subClassOf')
        prop_list = get_inverse_predicate_value(
            element_subclass + element, 'to:domain')

    ret_val = []
    for prop in prop_list:
        ret_val.append({
            'label': prop,
            'type': get_predicate_value(prop, 'to:subClassOf'),
            'ontolabel': get_predicate_value(prop, 'to:ontoLabel'),
            'ontodesc': get_predicate_value(prop, 'to:ontoDescription'),
        })

    return ret_val


def get_all_propeties_tree(element, retval={}):
    '''
        this method will return a tree structure as opposed to the method
        get_all_propeties. The output structure would be
        {
            <prop_name>: {
                ontolabel: '',
                ontodesc: '',
                range: '',
                children: {
                    <child_prop_name>: {
                        ontolabel: '',
                        ontodesc: '',
                        range: '',
                        children: ...
                    }
                }
            },

        }
    '''
    if not isinstance(element, list):
        element = [element]

    element_subclass = get_predicate_value(element, 'to:subClassOf')
    if collection.intersection(element_subclass, ['to:ComplexProperty', 'to:Entity']):
        pass

    pass
