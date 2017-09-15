import json
import os


DEFAULT_CONFIG = {
    "db": {
        "host": "localhost",
        "db": "toflerdb",
        "user": "user",
        "passwd": "password",
        "pool_size": 1
    },
    "elastic": {
        "host": "localhost",
        "port": 9200,
        "index": "toflerdb_snapshot",
        "doc_type": "node"
    },
    "redis": {
        "host": "localhost",
        "port": 6379
    }
}


def run():
    home = os.getenv('HOME')
    location = os.path.join(home, '.toflerdb.conf')
    if not os.path.isfile(location):
        f = open(location, 'w')
        f.write(json.dumps(DEFAULT_CONFIG, indent=4))
        f.close()
    else:
        print "File %s already exists" % location


if __name__ == '__main__':
    run()
