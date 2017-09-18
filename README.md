# ToflerDB

ToflerDB is Tofler's graph database engine that's being built to support
namespaces, annotated relationships, complex properties, history tracking
and field level security out of the box.

The database server exposes a simple interface accessible over HTTP making
it readily accessible from any language and even directly from a webpage.


## Introduction

There are some fairly popular graph databases out there already. So why another one? ToflerDB was born out of our need for several features that we just couldn't find in any of the existing solutions.

First of all, it had to do with how the data was modeled. In our mind, a node in the graph should always represent an *entity* of some kind. However, with most graph databases, complex relationships end up being nodes as well. We also wanted strong type-validation for data being stored in the database. To support this, we ended up needing to make some changes to RDF (*ouch!*) but it makes for a great data model. More on this elsewhere.

Another thing we needed was a mechanism to record the evolution of the graph. While typically no database gives you the history of how the data has changed over time, we realised that in several practical applications of a graph database, you'd want to be able to record things like when a particular fact was added, who added the fact and when it was modified or deleted. We wanted this to be a natural part of the system instead of a stuck-on artifact.

And finally, we wanted to have the ability to fine-tune the access rights to specific data fields. Unlike SQL-type databases where different types of facts about an entity can be stored in different tables, a graph database would keep all facts about an entity together. In such a scenario, the ability to restrict access to various fields is critical. Think of it as the ability to control which column of a table any given user is allowed to access.

We built ToflerDB to address these needs. Some of the features don't show up in this public version yet. This is because they were too tightly coupled with our internal systems and had to be stripped out. But we'll put them back again in a more generic fashion. There are also a bunch of other interesting features (like templatized IDs, throwaway IDs, etc) that you'll find when we talk about creating data for the database.


## Installation

For now, the system relies on MySQL, ElasticSearch and Redis as the underlying storage engines. To run ToflerDB, you need to have running instances of MySQL and ElasticSearch and also have the privileges to create the required tables and indices. You also need a running redis server to support record locking etc.

ToflerDB is distributed as a python package. It is self-sufficient except for a dependency on the python connector packages for MySQL, ElasticSearch and Redis.

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