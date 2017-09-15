from toflerdb.dbutils import dbutils


def get_all_namespaces():
    return dbutils.get_all_namespaces()


def get_all_entities_of_namespace(namespace):
    return dbutils.get_all_entities_of_namespace(namespace)


def get_all_propeties(elem):
    return dbutils.get_all_propeties(elem)
