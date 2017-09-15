import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.httputil
import json
import cgi
import datetime


class JSONHandler(tornado.httpserver.HTTPRequest):

    def __call__(self):
        self.stream.read_bytes(self.content_length, self.parse_json)

    def parse_json(self, data):
        print data
        try:
            json_data = json.loads(data)
        except ValueError:
            raise tornado.httpserver._BadRequestException(
                "Invalid JSON structure."
            )
        if type(json_data) != dict:
            raise tornado.httpserver._BadRequestException(
                "We only accept key value objects!"
            )
        for key, value in json_data.iteritems():
            self.request.arguments[key] = [value, ]
        self.done()


class CSRFHandler(tornado.web.RequestHandler):

    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers",
                        "x-requested-with,authorization,content-type")
        self.set_header('Access-Control-Allow-Methods',
                        'POST, GET, OPTIONS, PUT, DELETE')

    def options(self, *args):
        self.set_status(204)
        self.finish()


class XMLMaker(object):

    def __init__(self):
        pass

    def make_xml(self, obj, pretty=False):
        response = '<xml version="1.0" encoding="UTF-8">\n'
        obj = {'response': obj}
        indent = 0
        response += self._sub_make_xml(obj, indent=indent, pretty=pretty)
        return response

    def _sub_make_xml(self, obj, indent, pretty=False):
        def make_entry(k, obj, indent, pretty=False):
            newline = ''
            indent_close = False
            if not pretty:
                indent = 0
            if type(obj) is list or type(obj) is dict:
                subtree = self._sub_make_xml(
                    obj, indent=indent + 2, pretty=pretty)
                newline = '\n'
                indent_close = True
            elif type(obj) is datetime.date:
                if obj >= datetime.datetime(1900, 1, 1).date():
                    subtree = obj.strftime("%Y-%m-%d")
                else:
                    subtree = "%s-%02d-%02d" % (obj.year, obj.month, obj.day)
            elif type(obj) is datetime.datetime:
                if obj >= datetime.datetime(1900, 1, 1):
                    subtree = obj.strftime("%Y-%m-%d")
                else:
                    subtree = "%s-%02d-%02d" % (obj.year, obj.month, obj.day)
            else:
                if type(obj) is unicode:
                    subtree = obj.encode('utf-8')
                else:
                    subtree = str(obj)
            response = ' ' * indent
            response += '<%s>%s%s' % (k, newline, subtree)
            if indent_close:
                response += ' ' * indent
            response += '</%s>\n' % k
            return response

        response = ''
        if type(obj) is dict:
            for k in obj:
                response += make_entry(k, obj[k], indent, pretty)
        elif type(obj) is list:
            for item in obj:
                response += make_entry('item', item, indent, pretty)
        return response


class FloatEncoder(json.JSONEncoder):
    #Todo need docs what it does/when to use/why it is needed

    def __init__(self, nan_str="null", **kwargs):
        super(FloatEncoder, self).__init__(**kwargs)
        self.nan_str = nan_str

    def iterencode(self, o, _one_shot=False):
        """Encode the given object and yield each string
        representation as available.

        For example::

            for chunk in JSONEncoder().iterencode(bigobject):
                mysocket.write(chunk)
        """
        if self.check_circular:
            markers = {}
        else:
            markers = None
        if self.ensure_ascii:
            _encoder = json.encoder.encode_basestring_ascii
        else:
            _encoder = json.encoder.encode_basestring
        if self.encoding != 'utf-8':
            def _encoder(o, _orig_encoder=_encoder, _encoding=self.encoding):
                if isinstance(o, str):
                    o = o.decode(_encoding)
                return _orig_encoder(o)

        def floatstr(o, allow_nan=self.allow_nan, _repr=json.encoder.FLOAT_REPR,
                     _inf=json.encoder.INFINITY, _neginf=-json.encoder.INFINITY,
                     nan_str=self.nan_str):
            # Check for specials.  Note that this type of test is processor
            # and/or platform-specific, so do tests which don't depend on the
            # internals.

            if o != o:
                text = nan_str
            elif o == _inf:
                text = nan_str
            elif o == _neginf:
                text = nan_str
            else:
                return _repr(o)

            if not allow_nan:
                raise ValueError(
                    "Out of range float values are not JSON compliant: " +
                    repr(o))

            return text

        _iterencode = json.encoder._make_iterencode(
            markers, self.default, _encoder, self.indent, floatstr,
            self.key_separator, self.item_separator, self.sort_keys,
            self.skipkeys, _one_shot)
        return _iterencode(o, 0)


class BaseHandler(CSRFHandler):

    JSON = 'json'
    xmlmaker = XMLMaker()

    def _make_json_compatible(self, obj, pretty=False):
        dthandler = lambda obj: (self.jsonize_date(obj) if isinstance(
            obj, datetime.datetime) or isinstance(obj, datetime.date) else None)
        if pretty:
            return json.dumps(obj, default=dthandler, indent=4)
        else:
            return json.dumps(obj, cls=FloatEncoder, default=dthandler)

    def jsonize_date(self, date):
        if (type(date) == datetime.date):
            if (date >= datetime.datetime(1900, 1, 1).date()):
                return date.strftime("%Y-%m-%d")
            else:
                return "%s-%02d-%02d" % (date.year, date.month, date.day)
        elif (type(date) == datetime.datetime):
            if (date >= datetime.datetime(1900, 1, 1)):
                return date.strftime("%Y-%m-%d")
            else:
                return "%s-%02d-%02d" % (date.year, date.month, date.day)
        else:
            return ""

    def apiwrite(self, response, data_type=JSON, status=True):
        '''status is False if an error is thrown'''
        if data_type == self.JSON or data_type == self.XML:
            params = self.request.query
            data = cgi.parse_qs(params)
            format_needed = data.get('format', ['json'])[0]
            pretty = data.get('pretty', ['false'])[0]
            if pretty.lower() == 'true':
                pretty = True
            else:
                pretty = False
            if status:
                final_response = {'status': 'SUCCESS', 'content': response}
            else:
                final_response = {'status': 'FAILURE', 'error': response}
            if format_needed == 'xml':
                self.write(self.xmlmaker.make_xml(final_response, pretty))
            else:
                self.write(self._make_json_compatible(final_response, pretty))
