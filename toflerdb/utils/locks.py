from toflerdb.utils.common import Common


def is_locked(node):
    r = Common.get_cache_connection()
    return r.exists(node)


def lock_nodes(nodes):
    LOCK_INTERVAL = 5 * 60
    if not isinstance(nodes, list):
        nodes = [nodes]
    r = Common.get_cache_connection()
    for node in nodes:
        r.set(node, node, ex=LOCK_INTERVAL, nx=True)
    return nodes


def release_lock(nodes):
    r = Common.get_cache_connection()
    r.delete(nodes)
