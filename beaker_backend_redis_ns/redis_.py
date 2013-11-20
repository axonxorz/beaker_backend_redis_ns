import logging
from beaker.exceptions import InvalidCacheBackendError

from nosql import Container
from nosql import NoSqlManager
from nosql import pickle

try:
    from redis import StrictRedis, ConnectionPool
    import json
except ImportError:
    raise InvalidCacheBackendError("Redis cache backend requires the 'redis' library and JSON support")

log = logging.getLogger(__name__)
log.setLevel(logging.WARN)

class RedisManager(NoSqlManager):
    def __init__(self,
                 namespace,
                 url=None,
                 data_dir=None,
                 lock_dir=None,
                 **params):
        self.db = params.pop('db', None)
        self.dbpass = params.pop('password', None)
        self.connection_pools = {}

        if url is not None and ':' not in url:
            url = url + ':6379'

        self.localnamespace = params.pop('localnamespace', None)
        if self.localnamespace is None:
            raise InvalidCacheBackendError("Cannot initialize session. 'localnamespace' config value must be set")

        self.namespaces = params.pop('namespaces', '').split('\n')

        log.debug('Session namespaces: %s, %s' % (self.localnamespace, self.namespaces))

        self.expiretime = int(params.pop('expiretime', '43200'), 10)

        NoSqlManager.__init__(self,
                              namespace,
                              url=url,
                              data_dir=data_dir,
                              lock_dir=lock_dir,
                              **params)

    def open_connection(self, host, port, **params):
        pool_key = self._format_pool_key(host, port, self.db)
        if pool_key not in self.connection_pools:
            self.connection_pools[pool_key] = ConnectionPool(host=host,
                                                             port=port,
                                                             db=self.db,
                                                             password=self.dbpass)
        self.db_conn = StrictRedis(connection_pool=self.connection_pools[pool_key],
                                   **params)

    def __contains__(self, key):
        return self.db_conn.exists(self._format_key(key))

    def __getitem__(self, key):
        key = self._format_key(key)
        log.debug('Fetching key %s' % key)
        try:
            authNamespace = json.loads(self.db_conn.hget(key, 'Auth') or '{}')
            localNamespace = json.loads(self.db_conn.hget(key, self.localnamespace) or '{}')
            if authNamespace is None:
                authNamespace = {}
            if localNamespace is None:
                localNamespace = {}
            for otherNamespace in self.namespaces:
                ns = json.loads(self.db_conn.hget(key, otherNamespace) or '{}')
                localNamespace[otherNamespace] = ns

            localNamespace['Auth'] = authNamespace

            log.debug(localNamespace)
        except Exception, e:
            log.error(e)
            return None

        return localNamespace

    def set_value(self, key, value, expiretime=None):
        key = self._format_key(key)

        if expiretime is None and self.expiretime is not None:
            expiretime = self.expiretime

        authNamespace = value.get('Auth')
        if authNamespace is not None:
            del value['Auth']
            self.db_conn.hset(key, 'Auth', json.dumps(authNamespace))

        for otherNamespace in self.namespaces:
            ns = value.get(otherNamespace)
            if ns is not None:
                del value[otherNamespace]
            self.db_conn.hset(key, otherNamespace, json.dumps(ns))

        self.db_conn.hset(key, self.localnamespace, json.dumps(value))


        self.db_conn.expire(key, self.expiretime)

        if expiretime:
            self.db_conn.expire(key, self.expiretime)

    def __delitem__(self, key):
        # This is untested
        log.debug('Deleting key %s' % self._format_key(key))
        self.db_conn.delete(self._format_key(key))

    def _format_key(self, key):
        return 'sessions:%s' % (self.namespace)

    def _format_pool_key(self, host, port, db):
        return '{0}:{1}:{2}'.format(host, port, self.db)
    
    def do_remove(self):
        # This is untested/unmodified
        log.debug('Flushing session')
        self.db_conn.flush()

    def keys(self):
        log.debug('Listing keys')
        # This is untested/unmodified
        return self.db_conn.keys('sessions:%s:*' % self.namespace)


class RedisContainer(Container):
    namespace_class = RedisManager
