import hashlib


def merge_unique(arr1, arr2):
    return arr1 + [val for val in arr2 if val not in arr1]


def intersection(arr1, arr2):
    return [val for val in arr2 if val in arr1]


def subtract(arr1, arr2):
    return [val for val in arr1 if val not in arr2]


def is_sublist(arr, sub_arr):
    for e in sub_arr:
        if e not in arr:
            return False
    return True


def is_equal_list(arr1, arr2):
    return is_sublist(arr1, arr2) and is_sublist(arr2, arr1)


def is_empty_list(arr):
    if not arr:
        return True
    for elem in arr:
        if isinstance(elem, str) or isinstance(elem, unicode):
            elem = elem.strip()
        if elem:
            return False

    return True

# def find_path(element, JSON, path, all_paths):
#   if element in JSON:
#     path = path + element + ' = ' + JSON[element].encode('utf-8')
#     print path
#     all_paths.append(path)
#   for key in JSON:
#     if isinstance(JSON[key], dict):
#       find(element, JSON[key],path + key + '.',all_paths)


def add_fact_id(val, fid, is_unique=True):
    obj = {
        'fact_id': fid,
        'value': val
    }
    return obj if is_unique else [obj]


def found_id(JSON, _id):
    if isinstance(JSON, list):
        for j in JSON:
            if isinstance(j, dict) and j.get('id') == _id:
                return True
    elif isinstance(JSON, dict) and JSON.get('id') == _id:
        return True
    return False


def is_subsegment(JSON, segment):
    if not isinstance(segment, list):
        segment = [segment]

    for s in segment:
        if s.viewitems() <= JSON.viewitems():
            return True
    return False


def delete_segment(JSON, segment):
    """
    This method deletes the segment of the JSON object where the
    argument segment is a sub match in the JSON. It also clears out
    any key with empty elem.

    Example1:
        Input:
            JSON = {
                "key1" : "val1",
                "key2" : {
                    "key2.1" : "val2.1"
                }
            }
            segment = {"key2.1" : "val2.1"}
        Output:
            JSON = {
                "key1" : "val1",
            }

    Example2:
        Input:
            JSON = {
                "key1" : "val1",
                "key2" : [{
                    "key2_1.1" : "val2_1.1"
                }, {
                    "key2_2.1" : "val2_2.1"
                }]
            }
            segment = {"key2_1.1" : "val2_1.1"}
        Output:
            JSON = {
                "key1" : "val1",
                "key2" : [{
                    "key2_2.1" : "val2_2.1"
                }]
            }

    Example3:
        Input:
            JSON = {
                "key1" : "val1",
                "key2" : [{
                    "key2_1.1" : "val2_1.1"
                }, {
                    "key2_2.1" : "val2_2.1"
                }, {
                    "key2_3.1" : "val2_3.1",
                    "key2_3.2" : "val2_3.2"
                }, {
                    "key2_4.1" : "val2_4.1"
                }]
            }
            segment = [
                {"key2_1.1" : "val2_1.1"},
                {"key2_3.2" : "val2_3.2"}
            ]
        Output:
            JSON = {
                "key1" : "val1",
                "key2" : [{
                    "key2_2.1" : "val2_2.1"
                }, {
                    "key2_4.1" : "val2_4.1"
                }]
            }
    """
    if isinstance(JSON, dict) and is_subsegment(JSON, segment):
        return None
    elif isinstance(JSON, list):
        to_del_idx = []
        for idx in range(0, len(JSON)):
            if delete_segment(JSON[idx], segment) is None:
                to_del_idx.append(idx)

        idx_shift = 0
        for idx in to_del_idx:
            del JSON[idx - idx_shift]
            idx_shift += 1

    elif isinstance(JSON, dict):
        for key in JSON.keys():
            val = JSON[key]
            if delete_segment(val, segment) is None or \
                    (isinstance(val, list) and len(val) == 0):
                del JSON[key]

    return JSON


def find_path(JSON, key, _id=None):
    if isinstance(JSON, dict) and key in JSON and (
            _id is None or found_id(JSON[key], _id)):
        return [key]
    if isinstance(JSON, list):
        for j in JSON:
            return find_path(j, key, _id)
    if isinstance(JSON, dict):
        for k in JSON:
            path = find_path(JSON[k], key, _id)
            if path:
                return [k] + path


def assign_value_to_path(JSON, val, path):
    curr_json = JSON
    for key in path:
        if key not in curr_json:
            curr_json[key] = {}
        curr_json = curr_json[key]
    curr_json.update(val)


def _update_with_value(JSON, value):
    for k in value:
        v = value[k]
        if isinstance(v, list) and k in JSON:
            JSON[k] += v
        else:
            JSON[k] = v


def _direct_assign(JSON, value, _id=None):
    if isinstance(JSON, list):
        for j in JSON:
            if isinstance(j, dict):
                if _id is None:
                    _update_with_value(j, value)
                elif j.get('id') == _id:
                    _update_with_value(j, value)
    if isinstance(JSON, dict):
        if _id is None:
            _update_with_value(JSON, value)
        elif JSON.get('id') == _id:
            _update_with_value(JSON, value)


def assign_value(JSON, key, value, _id=None):
    if not isinstance(value, dict):
        raise ValueError('Value must be a dictionary')
    if isinstance(JSON, dict) and key in JSON:
        _direct_assign(JSON[key], value, _id)

    if isinstance(JSON, list):
        for j in JSON:
            assign_value(j, key, value, _id)

    if isinstance(JSON, dict):
        for k in JSON:
            assign_value(JSON[k], key, value, _id)


def get_datatype(ontology_types):
    if 'to:Date' in ontology_types:
        return {"type": "date"}
    if 'to:Integer' in ontology_types:
        return {"type": "integer"}
    if 'to:Float' in ontology_types:
        return {"type": "float"}
    if 'to:String' in ontology_types:
        return {"type": "string"}
    if 'to:Token' in ontology_types:
        return {"type": "string", "index": "not_analyzed"}
    return {"type": "string"}


def typecast(elem, ontology_types):
    if 'to:Date' in ontology_types:
        return elem
    if 'to:Integer' in ontology_types:
        return int(elem)
    if 'to:Float' in ontology_types:
        return float(elem)
    if 'to:String' in ontology_types:
        return str(elem)
    if 'to:Token' in ontology_types:
        return str(elem)
    return elem


def adjust_start_commit_text(text):
    if not text.startswith('START_TRANSACTION'):
        text = '%s\n%s' % ('START_TRANSACTION .', text)
    if not (text.endswith('COMMIT') or text.endswith('COMMIT .')):
        text = '%s\n%s' % (text, 'COMMIT .')

    return text


def generate_hash(text):
    return hashlib.sha256(text).hexdigest()


def nested_set(dic, keys, value):
    for key in keys[:-1]:
        dic = dic.setdefault(key, {})
    dic[keys[-1]] = value
