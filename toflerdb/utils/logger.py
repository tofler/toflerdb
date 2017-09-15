import logging
import datetime

# import awshelper

FORMAT = '%(asctime)-15s %(levelname)s %(funcName)s() : %(message)s'


class ElasticLogHandler(logging.Handler):
    """This class provides the ability to log directly to elastic search"""

    def __init__(self, elastic):
        logging.Handler.__init__(self)
        self.es = None
        # selfinfo = awshelper.get_instance_details()
        # self.ip = selfinfo['privateIp']
        self.elastic = elastic

    def prepare_record_data(self, record):
        record_data = {
            # 'machine_ip'    : self.ip,
            'created': datetime.datetime.fromtimestamp(record.created),
            'exc_info': record.exc_info,
            'exc_text': record.exc_text,
            'level_name': record.levelname,
            'filename': record.filename,
            'module': record.module,
            'funcname': record.funcName,
            'lineno': record.lineno,
            'msg': record.msg,
            'formatted_msg': self.format(record),
            'process': record.process,
        }
        return record_data

    def put_data(self, record_data, doc_type='superlogger'):
        if self.es is None:
            self.es = self.elastic.get_connection()
        self.es.index(index='logger', doc_type=doc_type, body=record_data)

    def emit(self, record):
        record_data = self.prepare_record_data(record)
        doc_type = 'superlogger'
        if 'audit' in record.args:
            doc_type = 'auditlogger'
            record_data['level_name'] = 'AUDIT'
        self.put_data(record_data, doc_type=doc_type)


class Logger(logging.getLoggerClass()):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    WARN = logging.WARN
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL

    def __init__(self, *args, **kwargs):
        super(Logger, self).__init__(*args, **kwargs)
        self.printing_enabled = False

    def enable_filelog(self, filename):
        flh = logging.FileHandler(filename)
        formatter = logging.Formatter(FORMAT)
        flh.setFormatter(formatter)
        self.addHandler(flh)

    def enable_printing(self):
        if self.printing_enabled:
            return
        slh = logging.StreamHandler()
        formatter = logging.Formatter(FORMAT)
        slh.setFormatter(formatter)
        self.addHandler(slh)
        self.printing_enabled = True

    def audit(self, msg, *args, **kwargs):
        self.critical(msg, {'audit': True}, *args, **kwargs)
