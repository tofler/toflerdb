import argparse
import tornado.ioloop
import tornado.web
from handlers.queryhandler import QueryHandler
from handlers.generalsearchhandler import GeneralSearchHandler
from handlers.uploadontologyhandler import UploadOntologyHandler
from handlers.uploadfactshandler import UploadFactsHandler


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--port',
        help='Port on which dbserver will run',
        type=int, default=8888
    )
    return parser.parse_args()


def make_app():

    query_endpoints = [
        (r'/query', QueryHandler),
        (r'/search', GeneralSearchHandler),
    ]
    user_endpoints = [
    ]
    upload_endpoints = [
        (r'/upload/ontology', UploadOntologyHandler),
        (r'/upload/facts', UploadFactsHandler),
    ]

    handlers = []
    handlers += query_endpoints
    handlers += user_endpoints
    handlers += upload_endpoints

    return tornado.web.Application(handlers)


def main():
    args = parse_arguments()
    print "Starting dbserver on port %s" % args.port
    app = make_app()
    app.listen(args.port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
