from elasticsearch import ElasticsearchException
from toflerdb.utils.common import Common
from toflerdb.config import SNAPSHOT_INDEX, SNAPSHOT_DOC_TYPE


def execute_query(query, **kwargs):
    es = Common.get_elasticsearch().get_connection()
    try:
        response = es.search(
            index=SNAPSHOT_INDEX, doc_type=SNAPSHOT_DOC_TYPE,
            body=query, **kwargs)
        return response
    except ElasticsearchException, e:
        Common.get_logger().error('Error ES query execution\n%s' % e)
    except ValueError, e:
        Common.log_error('Invalid response status received from elastic')


def execute_search_query(query, **kwargs):
    es = Common.get_elasticsearch().get_connection()
    try:
        response = es.search(
            index=SNAPSHOT_INDEX, doc_type=SNAPSHOT_DOC_TYPE,
            body=query, **kwargs)
        return response
    except ElasticsearchException, e:
        Common.get_logger().error('Error ES query execution\n%s' % str(e))


def insert_into_snapshot(inputs):
    data = []
    for item in inputs:
        data.append(inputs[item])
    Common.get_elasticsearch().put_bulk_data(
        SNAPSHOT_INDEX, SNAPSHOT_DOC_TYPE, data)


def delete_snapshot_nodes_by_id(id_list):
    if not isinstance(id_list, list):
        id_list = [id_list]

    es = Common.get_elasticsearch().get_connection()
    for i in id_list:
        try:
            es.delete(index=SNAPSHOT_INDEX, doc_type=SNAPSHOT_DOC_TYPE, id=i)
        except ElasticsearchException, e:
            Common.get_logger().error('Error deleting node<%s>\n%s' % (i, e))


def get_snapshot_nodes(**kwargs):
    for key, val in kwargs.iteritems():
        break

    if not isinstance(val, list):
        val = [val]
    es = Common.get_elasticsearch_connection()
    q_string_arr = []
    for elem in val:
        q_string_arr.append({
            "query_string": {
                "fields": [key, "*.%s" % key],
                "query": elem
            }
        })
    try:
        query = {
            "query": {
                "bool": {
                    "should": q_string_arr
                }
            }
        }
        response = es.search(index=SNAPSHOT_INDEX,
                             doc_type=SNAPSHOT_DOC_TYPE, body=query)
        entities = {}
        if response['hits']['total'] > 0:
            for hit in response['hits']['hits']:
                entities[hit['_id']] = hit['_source']
            return entities
        return None
    except Exception as e:
        logger = Common.get_logger()
        logger.error(str(e))
        return None


def find_nodes_with_incoming_references(nodeid):
    query = {"query": {"match": {"_all": nodeid}}}
    es = Common.get_elasticsearch_connection()
    response = es.search(index=SNAPSHOT_INDEX,
                         doc_type=SNAPSHOT_DOC_TYPE, body=query)
    # response = response[1]
    if response['hits']['total'] < 1:
        return []
    hits = response['hits']['hits']
    return hits


def get_related_nodes(nodeid):
    query = {"query": {"match": {"id": nodeid}}}
    es = Common.get_elasticsearch_connection()
    response = es.search(index=SNAPSHOT_INDEX,
                         doc_type=SNAPSHOT_DOC_TYPE, body=query)
    # response = response[1]
    if response['hits']['total'] < 1:
        return None
    node = response['hits']['hits'][0]
    references = _gather_references(node['_source'])
    if len(references) == 0:
        return None
    match_segments = []
    for ref in references:
        match_segments.append({"match": {"id": ref[1]}})

    query = {"query": {"bool": {"should": match_segments}}}
    response = es.search(index=SNAPSHOT_INDEX,
                         doc_type=SNAPSHOT_DOC_TYPE, body=query)
    # response = response[1]
    if response['hits']['total'] < 1:
        return None
    return {'relationships': references, 'nodes': response['hits']['hits']}


def _gather_references(node, field_name=None):
    references = []
    for f in node:
        if field_name is None:
            new_field_name = f
        else:
            new_field_name = "%s.%s" % (field_name, f)
        if f == 'value':
            if type(node[f]) is long or type(node[f]) is int:
                references.append((field_name, node[f]))
        elif type(node[f]) is list:
            for item in node[f]:
                references += _gather_references(item, new_field_name)
        elif type(node[f]) is dict:
            references += _gather_references(node[f], new_field_name)
    return references


def _create_freetextsearch_output(data, request, output):
    for f in request:
        if type(request[f]) is dict:
            if f not in data:
                continue
            op = {}
            if type(data[f]) is list:
                new_array = []
                for item in data[f]:
                    item.pop('fact_id')
                    new_array.append(item)
                _create_freetextsearch_output(new_array, request[f], op)
            else:
                data[f].pop('fact_id')
                _create_freetextsearch_output(data[f], request[f], op)
            output[f] = op
        else:
            if f in data:
                if type(data[f]) is list:
                    new_array = []
                    for item in data[f]:
                        new_array.append(item['value'])
                    output[f] = new_array
                else:
                    if f == 'id':
                        output[f] = data[f]
                    else:
                        output[f] = data[f]['value']
    if 'id' in output:
        output['id'] = str(output['id'])


def freetextsearch(query):
    elastic_query = {"query": {"match": {"_all": query}}}
    es = Common.get_elasticsearch_connection()
    response = es.search(index=SNAPSHOT_INDEX,
                         doc_type=SNAPSHOT_DOC_TYPE, body=elastic_query)
    output = []
    hits = response['hits']['hits']
    for hit in hits:
        src = hit['_source']
        opsrc = {}
        _create_freetextsearch_output(src, src, opsrc)
        output.append(opsrc)
    return output


def general_search(text):
    elastic_query = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"to:label.value": text}},
                    {"match": {"to:label.value.value": text}},
                    {"match": {"to:label.value.whitespace_edgegram": text}},
                    {"match": {"to:label.value.whitespace_keyword": text}}
                ],
                "minimum_should_match": 1
            }
        }
    }
    response = execute_search_query(elastic_query, size=25)
    retval = []
    for hit in response['hits']['hits']:
        src = hit['_source']
        item = {}
        item['id'] = src['id']
        item['to:label'] = []
        for label in src['to:label']:
            item['to:label'].append(label['value'])
        retval.append(item)

    return retval
