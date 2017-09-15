# ToflerDB

ToflerDB is Tofler's graph database engine that's being built to support
namespaces, annotated relationships, complex properties, history tracking
and field level security out of the box.

The database server exposes a simple interface accessible over HTTP making
it readily accessible from any language and even directly from a webpage.


## Getting Started ##

For now, the system relies on MySQL, ElasticSearch and Redis as the underlying
storage engines. To run ToflerDB, you need to have running instances of MySQL
and ElasticSearch and also have the privileges to create the required tables
and indices. You also need a running redis server to support record locking
etc.

ToflerDB is distributed as a python package. It is self-sufficient except for
a dependency on the python connector packages for MySQL, ElasticSearch and
Redis.

ToflerDB is hosted on pypi and can be installed as follows:

```pip install toflerdb --process-dependency-links```

The additional flag `--process-dependency-links` is required to allow the
installation of the `mysql-connector-python` package which is not available
through pypi.

The system is currently only tested on Ubuntu 14.04 and higher.

### Steps to get everything going ###
1. Install MySQL server
2. Install Redis
3. Install ElasticSearch 5.x
4. Install toflerdb python package
5. run `toflerdb-generate-config`. This will generate a default configuration in `.toflerdb.conf` in your home directory
5. run `toflerdb-dbinit`
6. run the server using `toflerdb-server` which runs on port 8888 by default