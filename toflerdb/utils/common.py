import logging
from .config import Configuration
from .db import Database
from .elastic import Elastic
from .logger import Logger, ElasticLogHandler, FORMAT
from .idmanagement import IDManager
from .cache import Redis


class Common(object):

    configuration = None
    database = None
    elastic = None
    logger = None
    idmanager = None
    redis = None

    @classmethod
    def load_config(cls, location=None):
        cls.configuration = Configuration(location)

    @classmethod
    def get_config(cls, section):
        if cls.configuration is None:
            cls.load_config()
        return cls.configuration.get_config(section)

    @classmethod
    def _get_database(cls):
        if cls.configuration is None:
            cls.load_config()
        if cls.database is None:
            cls.database = Database(
                pool_name='toflerdb',
                config=cls.configuration.get_config('db')
            )
        return cls.database

    @classmethod
    def execute_query(cls, query, params=None, **kwargs):
        if cls.database is None:
            cls._get_database()
        return cls.database.execute_query(query, params, **kwargs)

    @classmethod
    def _create_elastic_object(cls):
        if cls.elastic is None:
            if cls.configuration is None:
                cls.load_config()
            # Check if we have a configuration for es
            config = cls.configuration.get_config('elastic')
            cls.elastic = Elastic(config['host'], config['port'])

    @classmethod
    def get_elasticsearch_connection(cls):
        if cls.elastic is None:
            cls._create_elastic_object()
        return cls.elastic.get_connection()

    @classmethod
    def get_elasticsearch(cls):
        if cls.elastic is None:
            cls._create_elastic_object()
        return cls.elastic

    @classmethod
    def get_logger(cls, log_level=logging.INFO):
        if cls.logger is None:
            formatter = logging.Formatter(FORMAT)
            cls._create_elastic_object()
            elh = ElasticLogHandler(cls.elastic)
            elh.setFormatter(formatter)
            logging.setLoggerClass(Logger)
            logger = logging.getLogger('tofler')
            logger.setLevel(log_level)
            logger.addHandler(elh)
            cls.logger = logger
            return logger
        else:
            return cls.logger

    @classmethod
    def _get_idmanager(cls):
        if cls.idmanager is None:
            cls._get_database()
            cls.idmanager = IDManager(cls.database, cls.get_logger())
        return cls.idmanager

    @classmethod
    def generate_id(cls, namespace=None):
        idmanager = cls._get_idmanager()
        return idmanager.generate_new_id(namespace)

    @classmethod
    def _make_redis(cls):
        if cls.redis is not None:
            return
        if cls.configuration is None:
            cls.load_config()
        redis_config = cls.get_config('redis')
        cls.redis = Redis(config=redis_config)

    @classmethod
    def get_cache_connection(cls):
        if cls.redis is None:
            cls._make_redis()
        return cls.redis.get_connection()
