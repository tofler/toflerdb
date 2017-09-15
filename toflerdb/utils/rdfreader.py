import csv


class RDF(csv.Dialect):
    delimiter = ' '
    quotechar = '"'
    # escapechar = '"'
    lineterminator = '\n'
    quoting = csv.QUOTE_MINIMAL

csv.register_dialect('rdfttl', RDF)


def get_rdf_reader(file_handle):
    reader = csv.reader(file_handle, 'rdfttl')
    return reader


def get_rdf_writer(file_handle):
    writer = csv.writer(file_handle, 'rdfttl')
    return writer
