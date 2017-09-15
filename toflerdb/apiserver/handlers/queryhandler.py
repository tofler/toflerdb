# import tornado.web
import json
from toflerdb.core import api as gcc_api
from basehandler import BaseHandler


class UnauthorizedQuery(ValueError):
    '''Returned when query tries to access a resource not allowed for a user'''


class QueryHandler(BaseHandler):

    def post(self):
        return self.get()

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
        try:
            response = self.process_request(request_body)
            self.apiwrite(response)
        except UnauthorizedQuery:
            # TODO: find a way to specify the exact unauthorized fields
            self.apiwrite('Unauthorized Query', status=False)

    def screen_query(self, query):
        # All fields specified in the query must be
        # permitted for the user. otherwise, the query
        # will be rejected

        # Step 1 is to check for namespaces. All public
        # namespaces go through by default

        # Step 2 - for private namespaces, individual
        # types and properties are to be checked based
        # on dataset permissions. This can be cached
        # on a per-user basis?

        if 'to:type' in query:
            types = query['to:type']
            if type(types) is not list:
                types = list[types]

        elif 'id' not in query:
            query['to:type'] = 'to:Entity'

        return True

    def filter_responses(self, responses):
        return responses

    def process_request(self, request):
        #valid_query = self.screen_query(request)
        # if not valid_query:
        #    raise UnauthorizedQuery("Query to access non-permitted fields")
        '''
        if_event_type = False
        if 'to:type' in request:
            if request['to:type'] == 'gobiz:Investment'
                    or request['to:type'] == 'gomarkets:IndiaBulkDeal':
                if_event_type = True
        '''
        output = gcc_api.graph_query(request)
        filtered_output = self.filter_responses(output)
        return filtered_output
