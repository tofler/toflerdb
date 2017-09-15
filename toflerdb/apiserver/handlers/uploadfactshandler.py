import json
import traceback
# from toflerdb.utils.common import Common
from toflerdb.core import api as gcc_api
from toflerdb.utils import exceptions
from basehandler import BaseHandler


class UploadFactsHandler(BaseHandler):

    def post(self):
        request_body = self.request.body
        if request_body is None:
            return []
        response = []
        try:
            request_body = json.loads(request_body)
        except:
            print "Error processing request"
            response = []
        fact_tuples = request_body.get('fact_tuples', None)
        file_text = request_body.get('file_text', None)
        ignore_duplicate = request_body.get('ignore_duplicate', True)
        author = request_body.get('author', None)
        try:
            response = gcc_api.insert_facts(
                fact_tuples=fact_tuples, file_text=file_text, author=author,
                ignore_duplicate=ignore_duplicate)
            self.apiwrite(response)
        except exceptions.ToflerDBException, e:
            print traceback.format_exc()
            self.apiwrite(str(e), status=False)
        except Exception, e:
            print traceback.format_exc()
            # Common.get_logger().error(str(e))
            self.apiwrite('Something went wrong', status=False)
