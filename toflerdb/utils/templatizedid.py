def is_templatized_id(temp_id):
    try:
        retvalue = temp_id.startswith('_')
    except:
        return False
    return retvalue


def is_keep_id(temp_id):
    return is_templatized_id(temp_id) and not temp_id.startswith('__')


def append_userid(temp_id, uid):
    if not uid:
        raise Exception(
            'Use of templatized id without user id is strictly restricted')

    return '%s#%s' % (temp_id, uid)


def trim_userid(temp_ids, uid):
    temp_id_map = {}
    end_str = '#%s' % uid
    for i in temp_ids:
        val = temp_ids[i]
        if is_templatized_id(i) and i.endswith(end_str):
            temp_id_map[i.split(end_str)[0]] = val

    return temp_id_map
