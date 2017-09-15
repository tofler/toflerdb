import time


class IDManager(object):

    _MAX_WORKERS = 1024 * 8
    _LEASE_PERIOD = 60  # seconds
    _EVENTS_PER_SECOND = 400000

    ALPHABET = 'bcdfghjklmnpqrstvwxyz0123456789'
    ALPHABET_REVERSE = dict((c, i) for (i, c) in enumerate(ALPHABET))
    BASE = len(ALPHABET)

    def __init__(self, db, logger):
        self._counter = 0
        self._counter_reset_at = 0
        self._WORKER_ID = None
        self._db_lease_ts = None
        self._obtained_at = None
        self._db = db
        self._logger = logger
        self._obtain_new_worker_id()

    def _obtain_new_worker_id(self):
        query = """DELETE FROM toflerdb_worker WHERE
                assigned_at < NOW() - INTERVAL lease_period SECOND
                """
        self._db.execute_query(query)

        query = """SELECT worker_id FROM toflerdb_worker ORDER BY
                worker_id"""
        block_query = """INSERT INTO toflerdb_worker (worker_id) VALUES (%s)"""
        lease_start_tsquery = """SELECT assigned_at FROM toflerdb_worker
                    WHERE worker_id = %s"""
        candidate = 1
        while True:
            response = self._db.execute_query(query)
            if len(response) == self._MAX_WORKERS:
                self._logger.warn(
                    'Worker pool saturated. Sleeping for 1 second.')
                time.sleep(1)
            for res in response:
                if res['worker_id'] == candidate:
                    candidate += 1
                else:
                    break
            if candidate > self._MAX_WORKERS:
                self._logger.warn(
                    'Worker pool saturated. Sleeping for 1 second.')
                time.sleep(1)
                continue
            try:
                self._db.execute_query(block_query, candidate)
                self._WORKER_ID = candidate
                tsquery = self._db.execute_query(
                    lease_start_tsquery, candidate)
                self._db_lease_ts = tsquery[0]['assigned_at']
                self._obtained_at = time.time()
                break
            except:
                pass

    def _renew_lease(self):
        query = """UPDATE toflerdb_worker SET assigned_at = NOW()
                WHERE worker_id = %s AND assigned_at = %s"""
        response = self._db.execute_query(
            query, (self._WORKER_ID, self._db_lease_ts))
        if response['rows'] == 0:
            self._logger.warn(
                "Lease expired before renewal. Obtaining new lease.")
            self._obtain_new_worker_id()
            return
        self._obtained_at = time.time()
        query = """SELECT assigned_at FROM toflerdb_worker WHERE worker_id = %s"""
        response = self._db.execute_query(query, self._WORKER_ID)
        self._db_lease_ts = response[0]['assigned_at']
        self._logger.debug("Lease renewed for worker id %s" % self._WORKER_ID)

    def generate_new_id(self, namespace=None):
        now = time.time()
        if self._obtained_at + self._LEASE_PERIOD < now:
            self._renew_lease()
        timestamp = int(now * 1000)

        '''the following this is an interesting trick. We want to reset the
        counter every 1 ms. So, we multiply now by 1000 and take the floor.
        If that changes, we reset'''
        if timestamp > self._counter_reset_at:
            self._counter_reset_at = timestamp
            self._counter = 1
        if self._counter >= 4090:
            '''TODO: for now the counter is fine. But ensure that
            it resets by the next call'''
            time.sleep(1)   # TODO: 1s is too long. We want it to be 2-3 ms.
        candidate = (timestamp * self._MAX_WORKERS + self._WORKER_ID) * 4096 + self._counter
        self._counter += 1
        if namespace is None:
            return self.num_encode(candidate)
        return 't:%s/%s' % (namespace, self.num_encode(candidate))

    def num_encode(self, n):
        s = []
        while True:
            n, r = divmod(n, self.BASE)
            s.append(self.ALPHABET[r])
            if n == 0:
                break
        return ''.join(reversed(s))
