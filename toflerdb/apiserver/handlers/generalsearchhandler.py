import json
# from toflerdb.utils.common import Common
from toflerdb.dbutils import snapshot as snapshot_dbutils
from toflerdb.utils import exceptions
from basehandler import BaseHandler


class GeneralSearchHandler(BaseHandler):

    def post(self):
        self.get()

    def get(self):
        request_body = self.request.body
        if request_body is None:
            return []
        response = []
        try:
            request_body = json.loads(request_body)
        except:
            print "Error processing request"
            response = []
        search_text = request_body.get('q', None)
        try:
            response = snapshot_dbutils.general_search(search_text)
            self.apiwrite(response)
        except exceptions.ToflerDBException, e:
            self.apiwrite(str(e), status=False)
        except Exception, e:
            # Common.get_logger().error(str(e))
            self.apiwrite('Something went wrong', status=False)
