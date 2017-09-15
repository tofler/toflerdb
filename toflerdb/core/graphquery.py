'''GraphQuery

This module provides the core capabilities to process GraphQuery queries
to run on top of ToflerDB
'''

import json
from pydash import merge, get
from toflerdb.dbutils import dbutils
from toflerdb.dbutils import snapshot as snapshot_dbutils
from toflerdb.utils import collection


class GraphQuery(object):

    def __init__(self):
        self._predicate_values = {}
        self._to_delete = False
        self.agg_operators = ['count', 'avg', 'sum', 'groupBy', 'distinctCount', 'min', 'max']

    def _gather_info_from_toflerdb(self, elem, prop_list):
        (elem, op) = self._extract_operator(elem)
        ret_val = []
        if elem not in self._predicate_values:
            self._predicate_values[elem] = {}
        for prop in prop_list:
            if not prop in self._predicate_values[elem]:
                if prop == 'type':
                    self._predicate_values[elem][prop] = \
                        dbutils.get_predicate_value(elem, 'to:type')
                elif prop == 'subclass':
                    self._predicate_values[elem][prop] = \
                        dbutils.get_predicate_value(elem, 'to:subClassOf')
                elif prop == 'domain':
                    self._predicate_values[elem][prop] = \
                        dbutils.get_predicate_value(elem, 'to:domain', level=1)
                elif prop == 'range':
                    self._predicate_values[elem][prop] = \
                        dbutils.get_predicate_value(elem, 'to:range')
                elif prop == 'type_subclass':
                    (prop_type, ) = self._gather_info_from_toflerdb(
                        elem, ['type'])
                    self._predicate_values[elem][prop] = \
                        dbutils.get_predicate_value(prop_type, 'to:subClassOf')
                elif prop == 'domain_subclass':
                    (prop_domain, ) = self._gather_info_from_toflerdb(
                        elem, ['domain'])
                    self._predicate_values[elem][prop] = \
                        dbutils.get_predicate_value(prop_domain, 'to:subClassOf')
                elif prop == 'type_domain':
                    (prop_type, ) = self._gather_info_from_toflerdb(
                        elem, ['type'])
                    self._predicate_values[elem][prop] = \
                        dbutils.get_predicate_value(
                            prop_type, 'to:domain', level=1)
                elif prop == 'type_range':
                    (prop_type, ) = self._gather_info_from_toflerdb(
                        elem, ['type'])
                    self._predicate_values[elem][prop] = \
                        dbutils.get_predicate_value(prop_type, 'to:range')
                elif prop == 'subclass_inverse_domain':
                    (prop_subclass, ) = self._gather_info_from_toflerdb(
                        elem, ['subclass'])
                    self._predicate_values[elem][prop] = \
                        dbutils.get_inverse_predicate_value(
                            [elem] + prop_subclass, 'to:domain', level=1)
            ret_val.append(self._predicate_values[elem][prop])

        return tuple(ret_val)

    def _has_graph_filter(self, request):
        for f in request:
            if isinstance(request[f], dict):
                if self._has_graph_filter(request[f]):
                    return True
            elif request[f] is not None:
                return True
        return False

    def _prepare_one_hop_request_body(self, elem, request):
        (elem_inverse_domain, ) = self._gather_info_from_toflerdb(
            elem, ['subclass_inverse_domain'])
        newrequest = {}
        for f in request:
            (f_ex, op) = self._extract_operator(f)
            if f_ex not in elem_inverse_domain:
                newrequest[f] = request[f]

        return newrequest

    def _prepare_direct_request_body(self, elem, request):
        (elem_inverse_domain, ) = self._gather_info_from_toflerdb(
            elem, ['subclass_inverse_domain'])
        newrequest = {}
        for f in request:
            (f_ex, op) = self._extract_operator(f)
            if f_ex in elem_inverse_domain:
                newrequest[f] = request[f]

        return newrequest

    def _extract_operator(self, f):
        all_operators = [
            '~|=', '|=', '~=', '>', '>=', '<',
            '<=', '~[]=', '~[]', '[]=', '[]'] + self.agg_operators

        op = None
        fld = f
        for o in all_operators:
            if f.endswith(o):
                op = o
                fld = f.split(o)[0].strip()
                break

            if f.startswith(o):
                op = o
                fld = f.rsplit("(")[1].strip().rsplit(")", 1)[0].strip()
                break
        return (fld, op)

    def _create_leaf_match_query(self, fld, fld_val):
        ''' this method checks for to:label field and create match
        query accordingly
        '''
        leaf_match_query = None
        if fld == 'to:label.value':
            leaf_match_query = {
                'bool': {
                    'should': [
                        {"match": {fld: fld_val}},
                        {"match": {"%s.value" % fld: fld_val}},
                        {"match": {"%s.whitespace_edgegram" % fld: fld_val}},
                        {"match": {"%s.whitespace_keyword" % fld: fld_val}}
                    ],
                    'minimum_should_match': 1
                }
            }
        else:
            leaf_match_query = {'match': {fld: fld_val}}
        return leaf_match_query

    def _operator_query_builder(self, op, fld, fld_val, org_fld, base_fld):
        op_query = None
        is_agg_query = is_nested_query = False
        if op is None:
            op_query = self._create_leaf_match_query(fld, fld_val)
        elif op == '~|=':
            op_query = {'bool': {'must_not': []}}
            if not isinstance(fld_val, list):
                fld_val = [fld_val]
            for fv in fld_val:
                op_query['bool']['must_not'].append(
                    self._create_leaf_match_query(fld, fv))
        elif op == '~=':
            op_query = {
                'bool': {
                    'must_not': [self._create_leaf_match_query(fld, fld_val)]
                }
            }
        elif op == '|=':
            op_query = {
                'bool': {
                    'should': [],
                    'minimum_should_match': 1
                }
            }
            if not isinstance(fld_val, list):
                fld_val = [fld_val]
            for fv in fld_val:
                op_query['bool']['should'].append(
                    self._create_leaf_match_query(fld, fv))
        elif op == '>=':
            op_query = {
                'range': {
                    fld: {
                        'gte': fld_val
                    }
                }
            }
        elif op == '>':
            op_query = {
                'range': {
                    fld: {
                        'gt': fld_val
                    }
                }
            }
        elif op == '<=':
            op_query = {
                'range': {
                    fld: {
                        'lte': fld_val
                    }
                }
            }
        elif op == '<':
            op_query = {
                'range': {
                    fld: {
                        'lt': fld_val
                    }
                }
            }
        elif op == '~[]=':
            op_query = {
                'bool': {
                    'should': [
                        {'range': {fld: {'lt': fld_val[0]}}},
                        {'range': {fld: {'gt': fld_val[1]}}}
                    ],
                    'minimum_should_match': 1
                }
            }
        elif op == '[]=':
            op_query = {
                'range': {
                    fld: {
                        'gte': fld_val[0],
                        'lte': fld_val[1]
                    }
                }
            }
        elif op == '~[]':
            op_query = {
                'bool': {
                    'should': [
                        {'range': {fld: {'lte': fld_val[0]}}},
                        {'range': {fld: {'gte': fld_val[1]}}}
                    ],
                    'minimum_should_match': 1
                }
            }
        elif op == '[]':
            op_query = {
                'range': {
                    fld: {
                        'gt': fld_val[0],
                        'lt': fld_val[1]
                    }
                }
            }
        elif op in self.agg_operators:
            is_agg_query = True
            if op == 'groupBy':
                op_query = {
                    fld: {
                        "terms": {
                            "field": fld
                        }
                    }
                }
            elif op == 'count':
                op_query = {
                    fld: {
                        "value_count": {
                            "field": fld
                            # "precision_threshold": 100,
                            # "rehash": false
                        }
                    }
                }
            elif op == 'distinctCount':
                op_query = {
                    fld: {
                        "cardinality": {
                            "field": fld
                            # "precision_threshold": 100,
                            # "rehash": false
                        }
                    }
                }
            elif op == 'avg':
                op_query = {
                    fld: {
                        "avg": {
                            "field": fld
                        }
                    }
                }
            elif op == 'sum':
                op_query = {
                    fld: {
                        "sum": {
                            "field": fld
                        }
                    }
                }
            elif op == 'min':
                op_query = {
                    fld: {
                        "top_hits": {
                            "size": 1,
                            "_source": {
                                "includes": [
                                    base_fld + "id",
                                    fld
                                ]
                            },
                            "sort": {
                                fld: "asc"
                            }
                        }
                    },
                }
            elif op == 'max':
                op_query = {
                    fld: {
                        "top_hits": {
                            "size": 1,
                            "_source": {
                                "includes": [
                                    base_fld + "id",
                                    fld
                                ]
                            },
                            "sort": {
                                fld: "desc"
                            }
                        }
                    },
                }

        (fld_domain_subclass,) = self._gather_info_from_toflerdb(org_fld, ['domain_subclass'])
        # print "base fld", fld, op, org_fld, fld_domain_subclass, base_fld
        if collection.intersection(fld_domain_subclass, [
                'to:ComplexRelationalProperty', 'to:ComplexProperty']):
            is_nested_query = True
            if base_fld.endswith('.'):
                base_fld = base_fld[:-1]
            if is_agg_query:
                op_query = {
                    base_fld+"-nested": {
                        "nested": {
                            "path": base_fld
                        },
                        "aggs": op_query
                    }
                }
            else:
                op_query = {
                    "nested": {
                        "path": base_fld.split(".")[-1],
                        "query": op_query,
                        "inner_hits": {}
                    }
                }

        return is_agg_query, is_nested_query, op_query

    def _get_field_name(self, f, base_string):

        if f == 'id':
            field_name = base_string + f
        elif f == 'value':
            field_name = base_string + f
        else:
            field_name = base_string + f + ".value"
        return field_name

    def _create_match_array(self, request, match_array, agg_query, base_string=""):
        nested_queries = {}
        # is_agg_query_scope = any([key.startswith(
        #     op) for op in self.agg_operators for key in request])
        for f in request:
            f_val = request[f]
            (f, op) = self._extract_operator(f)
            if f_val is None and op is None:
                continue

            if type(f_val) is dict:
                newrequest = f_val
                (f_subclass, ) = self._gather_info_from_toflerdb(
                    f, ['subclass'])
                if collection.intersection(f_subclass, [
                        'to:ComplexRelationalProperty',
                        'to:RelationalProperty']):
                    newrequest = self._prepare_direct_request_body(
                        f, f_val)

                new_agg_query = {}
                child_nested_queries = self._create_match_array(
                    newrequest, match_array, new_agg_query, base_string + f + ".")

                if new_agg_query:
                    agg_query.setdefault("aggs", {}).update(new_agg_query)

                if child_nested_queries:
                    grp_name = base_string.replace(".", "_") if base_string else "root"
                    agg_query.setdefault(grp_name, {
                        "terms": {"field": "id"},
                        "aggs": {}
                    })["aggs"].update(child_nested_queries)

            else:
                field_name = self._get_field_name(f, base_string)
                is_agg_query, is_nested_query, op_query = self._operator_query_builder(
                    op, field_name, f_val, f, base_string)
                if not is_agg_query:
                    match_array.append(op_query)
                else:
                    #for merging if agg belongs to same nested scope
                    if is_nested_query:
                        merge(nested_queries, op_query)
                    else:
                        agg_query.update(op_query)

        return nested_queries

    def _key_in_aggdata(self, key, aggdata, refval):
        for k in aggdata:
            if k == key:
                refval[0] = aggdata[k]
                return True
            elif isinstance(aggdata[k], dict) and self._key_in_aggdata(key, aggdata[k], refval):
                return True
            elif isinstance(aggdata[k], list):
                for item in aggdata[k]:
                    if isinstance(item, dict) and self._key_in_aggdata(key, item, refval):
                        return True
        return None

    def _create_aggregation_output(self, request, aggdata, base_string):
        op = {}
        has_agg_req_keys = False

        for agg_req_key in request.keys():
            (f, operator) = self._extract_operator(agg_req_key)
            field_name = self._get_field_name(f, base_string)
            refval = [None]
            if operator in self.agg_operators:
                has_agg_req_keys = True
                if aggdata and self._key_in_aggdata(field_name, aggdata, refval):
                    agg_value = refval[0]

                    # if :
                    #     #list
                    # else:
                        #not list

                    if operator in ['min', 'max']:
                        agg_value = get(refval[0], "hits.hits[0]._source.%s" % (f), None)
                        if agg_value:
                            op.update({"id": get(refval[0], "hits.hits[0]._source.id", None)})
                    op[agg_req_key] = agg_value

        return has_agg_req_keys, op

    def _create_output(self, data, request, output, aggdata, base_string=""):
        if self._to_delete:
            return

        for f in request:
            f_val = request[f]
            (f, op) = self._extract_operator(f)
            if type(f_val) is dict:
                if f not in data:
                    if self._has_graph_filter(f_val):
                        self._to_delete = True
                        return
                    continue
                op = None
                '''
                if f is a subclassof of either to:ComplexRelationalProperty
                or to:RelationalProperty,
                query all the ids in within itself and replace those inplace
                with associated
                '''
                (f_subclass, ) = self._gather_info_from_toflerdb(
                    f, ['subclass'])
                if collection.intersection(f_subclass, [
                        'to:ComplexRelationalProperty',
                        'to:RelationalProperty']):
                    if type(data[f]) is list:
                        #multiple object return case
                        newdata = []
                        hop_ids = []
                        hop_ids = [item['value'] for item in data[f]
                                   if item['value'] not in hop_ids]
                        filter_id = f_val.get('id', None)
                        newrequest = None
                        if filter_id and filter_id in hop_ids:
                            #fetch filtered id
                            newrequest = self._prepare_one_hop_request_body(
                                f, f_val)
                            newrequest.update({'id': filter_id})
                        elif not filter_id:
                            #fetch all ids
                            newrequest = self._prepare_one_hop_request_body(
                                f, f_val)
                            newrequest.update({'id|=': hop_ids})

                        if newrequest:
                            newresponse, newresponse_aggdata = self.query(
                                newrequest, raw_output=True)
                            if len(newresponse):
                                newresponse_normalized = {}
                                for nr in newresponse:
                                    newresponse_normalized[nr['id']] = nr
                                for item in data[f]:
                                    if item['value'] in newresponse_normalized:#checking id in newresponse_normalized
                                        item.update(newresponse_normalized[
                                            item['value']])
                                        newdata.append(item)

                        if len(newdata):
                            data[f] = newdata
                        else:
                            self._to_delete = True
                            return
                    else:
                        #single object return case
                        filter_id = f_val.get('id', None)
                        hop_id = data[f]['value']
                        #ignore unmatched ones
                        if filter_id and hop_id != filter_id:
                            continue
                        newrequest = self._prepare_one_hop_request_body(
                            f, f_val)
                        newrequest.update({'id': hop_id})
                        newresponse, newresponse_aggdata = self.query(
                            newrequest, raw_output=True)
                        if len(newresponse):
                            data[f].update(newresponse[0])
                        else:
                            self._to_delete = True
                            return

                    has_agg_req_keys, op = self._create_aggregation_output(f_val, newresponse_aggdata, "")
                else:
                    #aggdata so replacing all with aggdata
                    has_agg_req_keys, op = self._create_aggregation_output(f_val, aggdata, base_string+f+".")

                if not has_agg_req_keys:
                    if type(data[f]) is list:
                        # print 'list when val is dict', f
                        op = []
                        for item in data[f]:
                            if 'fact_id' in item:
                                item.pop('fact_id')
                            new_op = {}
                            self._create_output(item, f_val, new_op, aggdata, base_string + f + ".")
                            op.append(new_op)
                    else:
                        op = {}
                        data[f].pop('fact_id')
                        # print 'val when val is dict', f
                        self._create_output(data[f], f_val, op, base_string + f + ".")
                output[f] = op

            else:

                if f in data:
                    if type(data[f]) is list:
                        # print 'list when val is not dict', f
                        new_array = []
                        for item in data[f]:
                            if f == 'value':
                                new_array.append(item)
                            else:
                                new_array.append(item['value'])
                        output[f] = new_array
                    else:
                        # print 'val when val is not dict', f
                        if f == 'id' or f == 'value':
                            output[f] = data[f]
                        else:
                            output[f] = data[f]['value']
                else:
                    print f, 'not in data when val is not dict'

    def query(self, request, raw_output=False):
        match_array = []
        agg_query = {}
        output = []
        self._create_match_array(request, match_array, agg_query)
        elastic_query = {
            "query": {
                "bool": {
                    "must": match_array
                }
            }
        }
        if agg_query:
            elastic_query["aggs"] = agg_query

        print "req", request, elastic_query
        try:
            response = snapshot_dbutils.execute_search_query(
                elastic_query, size=10)
            aggout = response.get("aggregations")
            print "aggregations", aggout, len(response['hits']['hits'])
        except Exception as e:
            print str(e)

        has_agg_req_keys, op = self._create_aggregation_output(request, aggout, "")
        if has_agg_req_keys and not raw_output:
            output.append(op)
        else:
            hits = response['hits']['hits']
            for hit in hits:
                self._to_delete = False
                src = hit['_source']
                if 'inner_hits' in hit:
                    for nested_path in hit['inner_hits']:
                        src[nested_path] = [
                            nested_hit['_source']
                            for nested_hit in
                            hit['inner_hits'][nested_path]['hits']['hits']]
                opsrc = {}
                if not raw_output:
                    ## pass aggout
                    self._create_output(src, request, opsrc, aggout)
                    if not self._to_delete:
                        output.append(opsrc)
                else:
                    output.append(src)

        if not raw_output:
            return output
        else:
            return output, aggout

