import StringIO
import sys
# from toflerdb.utils.common import Common
from toflerdb.dbutils import eternity as eternity_dbutils
from toflerdb.utils import rdfreader, collection, exceptions, templatizedid
from onto_maker import OntoMaker
from entry_maker import EntryMaker
from eraser import Eraser
from graphquery import GraphQuery


def draw_progress_bar(percent, barlen=40):
    print "\r",
    progress = ""
    for i in range(barlen):
        if i < int(barlen * percent):
            progress += "="
        else:
            progress += " "
    print "[ %s ] %.2f%%" % (progress, percent * 100),
    sys.stdout.flush()


def _gather_fact_info(fact, author=None):
    '''
    this is method finds fact_id given fact_tuple or vice versa
    '''
    if isinstance(fact, tuple):
        fact_tuple = fact
        fact_id = eternity_dbutils.get_fact_ids([fact], author)
        if len(fact_id):
            fact_id = fact_id[0]
    else:
        fact_id = fact
        fact_tuple = eternity_dbutils.get_fact_tuples_by_fact_ids(fact_id)
        if len(fact_tuple):
            fact_tuple = fact_tuple[0]

    return (fact_id, fact_tuple)


def _insert_ontology_from_file_stream(
        file_stream, author=None, validation=True):
    onto_tuples = []
    reader = rdfreader.get_rdf_reader(file_stream)
    curr_line = 0
    for row in reader:
        curr_line += 1
        if collection.is_empty_list(row):
            continue
        if row[0].startswith('#'):
            continue
        if row[0].startswith('START_TRANSACTION'):
            continue
        if row[0].startswith('COMMIT'):
            _insert_ontology_from_tuples(
                onto_tuples, author=author, validation=validation)
            onto_tuples = []
            continue
        onto_tuples.append((row[0], row[1], row[2]))


def _insert_ontology_from_file_text(
        file_text, author=None, validation=None):
    file_text = collection.adjust_start_commit_text(file_text)

    csvfile = StringIO.StringIO(file_text)
    _insert_ontology_from_file_stream(
        csvfile, author=author, validation=validation)


def _insert_ontology_from_file(
        file, author=None, validation=True):
    with open(file, 'rb') as csvfile:
        _insert_ontology_from_file_stream(
            csvfile, author=author, validation=validation)


def _insert_ontology_from_tuples(onto_tuples, author=None, validation=True):
    om = OntoMaker()
    # currently validation is not supported in ontology
    # so we need to turn off validation always
    if validation:
        om.turn_on_validation()
    else:
        om.turn_off_validation()
    for row in onto_tuples:
        om.add_input(row[0], row[1], row[2])

    om.make_mapping()
    om.commit()


def insert_ontology(onto_tuples=None, file=None, file_text=None,
                    author=None, validation=True):
    if onto_tuples:
        _insert_ontology_from_tuples(onto_tuples, author, validation)
    if file:
        _insert_ontology_from_file(file, author, validation)
    if file_text:
        _insert_ontology_from_file_text(
            file_text, author, validation)


def _insert_facts_from_tuples(
        fact_tuples, author=None, validation=True, **kwargs):
    if not isinstance(fact_tuples, list):
        fact_tuples = [fact_tuples]

    ignore_duplicate = kwargs.get('ignore_duplicate', True)
    em = EntryMaker(author=author, ignore_duplicate=ignore_duplicate)
    if validation:
        em.turn_on_validation()
    else:
        em.turn_off_validation()

    for fact_tuple in fact_tuples:
        em.add_input(*fact_tuple)

    em.commit()
    response = {}
    response['templatized_id_map'] = templatizedid.trim_userid(
        em._templatized_id_map, author)
    return response


def _insert_facts_from_file_stream(
        file_stream, author=None, validation=True, **kwargs):
    response = {}
    templatized_id_map = {}
    fact_tuples = []
    reader = rdfreader.get_rdf_reader(file_stream)
    line_num = 0
    if kwargs.get('show_progress'):
        reader = list(reader)
        total_len = len(reader)

    for row in reader:
        line_num += 1
        if kwargs.get('start_pos') and line_num < kwargs['start_pos']:
            continue
        if collection.is_empty_list(row):
            continue
        if row[0].startswith('#'):
            continue
        if row[0].startswith('START_TRANSACTION'):
            continue
        if row[0].startswith('COMMIT'):
            if kwargs.get('show_progress'):
                draw_progress_bar((line_num * 1.0) / total_len)
            res = _insert_facts_from_tuples(
                fact_tuples, author=author, validation=validation, **kwargs)
            templatized_id_map.update(res['templatized_id_map'])
            response.update(res)
            if kwargs.get('facts_did_commit'):
                commit_info = {}
                commit_info['commit_pos'] = line_num
                commit_info.update(kwargs.get('callback_info', {}))
                kwargs['facts_did_commit'](commit_info)
            fact_tuples = []
            continue
        fact_tuples.append((row[0], row[1], row[2]))

    response['templatized_id_map'] = templatized_id_map
    return response


def _insert_facts_from_file_text(
        file_text, author=None, validation=None, **kwargs):
    file_text = collection.adjust_start_commit_text(file_text)
    csvfile = StringIO.StringIO(file_text)
    return _insert_facts_from_file_stream(
        csvfile, author=author, validation=validation, **kwargs)


def _insert_facts_from_file(file, author=None, validation=None, **kwargs):
    with open(file, 'rb') as csvfile:
        return _insert_facts_from_file_stream(
            csvfile, author=author, validation=validation, **kwargs)


def _insert_facts_from_s3_file(s3_key, author=None, validation=True, **kwargs):
    # file_text = Common.get_document_from_s3(s3_key)
    csvfile = StringIO.StringIO(file_text)
    response = _insert_facts_from_file_stream(
        csvfile, author=author, validation=validation, **kwargs)
    csvfile.close()
    return response


def insert_facts(
        fact_tuples=None, file=None, file_text=None,
        s3=None, author=None, validation=True, **kwargs):
    if not author:
        raise exceptions.MissingRequiredFieldError(
            'Inserting fact without author key is not allowed')

    if fact_tuples:
        return _insert_facts_from_tuples(
            fact_tuples, author, validation, **kwargs)
    if file:
        return _insert_facts_from_file(file, author, validation, **kwargs)
    if file_text:
        return _insert_facts_from_file_text(
            file_text, author, validation, **kwargs)
    if s3:
        return _insert_facts_from_s3_file(s3, author, validation, **kwargs)


def _update_facts_from_value_map(fact_values, author=None):
    fact_ids = []
    fact_tuples = []
    for fact in fact_values:
        (fact_id, fact_tuple) = _gather_fact_info(fact, author)
        fact_ids.append(fact_id)
        fact_tuples.append(
            (fact_tuple[0], fact_tuple[1], fact_values[fact], fact_id))

    erase_facts(fact_ids, author=author)
    insert_facts(fact_tuples, author=author)


def _update_facts_from_file(file, author=None):
    fact_values = {}
    with open(file, 'rb') as csvfile:
        reader = rdfreader.get_rdf_reader(csvfile)
        for row in reader:
            if collection.is_empty_list(row):
                continue
            if row[0].startswith('#'):
                continue
            if row[0].startswith('START_TRANSACTION'):
                continue
            if row[0].startswith('COMMIT'):
                _update_facts_from_value_map(fact_values, author=author)
                fact_values = {}
                continue
            if len(row) >= 4:
                fact_values[(row[0], row[1], row[2])] = row[3]
            elif len(row) >= 2:
                fact_values[row[0]] = row[1]


def update_facts(fact_values=None, file=None, author=None):
    if fact_values:
        _update_facts_from_value_map(fact_values, author)
    if file:
        _update_facts_from_file(file, author)


def _erase_facts_by_ids(fact_ids, author=None):
    if eternity_dbutils.is_any_type_facts(fact_ids):
        raise exceptions.InvalidInputValueError(
            'Not allowed to delete any to:type')
    else:
        ers = Eraser(author=author)
        ers.erase_facts(fact_ids)


def _erase_nodes_by_ids(node_ids, author=None):
    ers = Eraser(author=author)
    ers.erase_nodes(node_ids)


def _erase_facts_by_tuples(fact_tuples, author=None):
    fact_ids = eternity_dbutils.get_fact_ids(fact_tuples, author)
    _erase_facts_by_ids(fact_ids, author=author)


def _erase_facts_from_file(file, author=None):
    fact_ids = []
    fact_tuples = []
    with open(file, 'rb') as csvfile:
        reader = rdfreader.get_rdf_reader(csvfile)
        for row in reader:
            if collection.is_empty_list(row):
                continue
            if row[0].startswith('#'):
                continue
            if row[0].startswith('START_TRANSACTION'):
                continue
            if row[0].startswith('COMMIT'):
                if fact_ids:
                    _erase_facts_by_ids(fact_ids, author=author)
                if fact_tuples:
                    _erase_facts_by_tuples(fact_tuples, author=author)
                fact_ids = []
                fact_tuples = []
                continue
            if len(row) >= 3:
                fact_tuples.append((row[0], row[1], row[2]))
            elif len(row) >= 1:
                fact_ids.append(row[0])


def erase_facts(
        fact_ids=None, node_ids=None, fact_tuples=None,
        file=None, author=None):
    if fact_ids:
        _erase_facts_by_ids(fact_ids, author)
    if node_ids:
        _erase_nodes_by_ids(node_ids, author)
    if fact_tuples:
        _erase_facts_by_tuples(fact_tuples, author)
    if file:
        _erase_facts_from_file(file, author)


def graph_query(body):
    ql = GraphQuery()
    return ql.query(body)
