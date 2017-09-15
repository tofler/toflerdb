import redis


class Redis(object):

    def __init__(self, config):
        self._pool = redis.ConnectionPool(
            host=config['host'],
            port=config['port'],
            db=0
            )

    def set(self, key, value):
        r = redis.Redis(connection_pool=self._pool)
        return r.set(key, value)

    def get(self, key):
        r = redis.Redis(connection_pool=self._pool)
        return r.get(key)

    def incr(self, key):
        r = redis.Redis(connection_pool=self._pool)
        return r.incr(key)

    def get_connection(self):
        return redis.Redis(connection_pool=self._pool)
