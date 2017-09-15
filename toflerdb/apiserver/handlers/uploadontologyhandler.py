import json
import traceback
from toflerdb.core import api as gcc_api
from toflerdb.utils import exceptions
from basehandler import BaseHandler


class UploadOntologyHandler(BaseHandler):

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
        onto_tuples = request_body.get('onto_tuples', None)
        onto_text = request_body.get('file_text', None)
        try:
            response = gcc_api.insert_ontology(
                onto_tuples=onto_tuples, file_text=onto_text)
            self.apiwrite(response)
        except exceptions.ToflerDBException, e:
            print traceback.format_exc()
            self.apiwrite(str(e), status=False)
        except:
            print traceback.format_exc()
            self.apiwrite('Something went wrong', status=False)
