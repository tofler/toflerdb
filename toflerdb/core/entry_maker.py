import time
from toflerdb.utils.common import Common
from toflerdb.utils import exceptions
from toflerdb.dbutils import dbutils
from toflerdb.dbutils import snapshot as snapshot_dbutils
from toflerdb.dbutils import eternity as eternity_dbutils
from toflerdb.utils import collection
from toflerdb.utils import templatizedid
from toflerdb.utils import locks


class EntryMaker(object):

    def __init__(self, **kwargs):
        self._sql_query_list = []
        self._elastic_query_list = []
        self._entity_id = None
        self._entity_type_list = None
        self._input_list = []
        self._normalized_input = {}
        self._stores = {
            'snapshot': {},
            'inmemory': {}
        }
        self._fact_tuple = None
        self._templatized_id_map = {}
        self._new_templatized_id = []
        self._validation = True
        self._fact_id = None
        self._author = kwargs.get('author')
        self._locked_nodes = []
        self._ignore_duplicate = kwargs.get('ignore_duplicate', True)

    def turn_off_validation(self):
        self._validation = False
        return self

    def turn_on_validation(self):
        self._validation = True
        return self

    def normalize_input(self, subj, pred, objc):
        if subj not in self._normalized_input:
            self._normalized_input[subj] = {}
        if pred not in self._normalized_input[subj]:
            self._normalized_input[subj][pred] = []
        if objc not in self._normalized_input[subj][pred]:
            self._normalized_input[subj][pred].append(objc)

    def delete_normalize_input(self, subj, pred, objc):
        self._normalized_input[subj][pred].remove(objc)

    def add_fact_id(self, val, is_unique, is_type=False):
        obj = {
            'fact_id': self._fact_id,
            'value': val if not is_type else self.add_superclasses(val)
        }
        return obj if is_unique else [obj]

    def typecast_input(self, subj, pred, objc):
        # subj = int(subj)
        if pred == 'to:type':
            return objc
        # if not dbutils.exists_in_eternity(pred, ontology_only = True):
        #     pred = int(pred)
        (pred_subclass, pred_type_subclass, pred_range, pred_type_range) = \
            self.gather_info_from_toflerdb(pred, [
                'subclass', 'type_subclass', 'range', 'type_range'])
        if collection.intersection(
                ['to:RelationalProperty', 'to:ComplexRelationalProperty'],
                pred_subclass + pred_type_subclass):
            objc = str(objc)
        else:
            objc = collection.typecast(objc, pred_range + pred_type_range)
        return objc

    def add_templatized_id_map(self, temp_id):
        '''
        create the templatized_id to tofler_id map, if not exists
        returns the tofler_id
        if the temp_id is not a throw away id, look for it in eternity
        if found, cache the mapping, else create a new id
        '''
        if temp_id not in self._templatized_id_map:
            stored_id = []
            if templatizedid.is_keep_id(temp_id):
                stored_id = dbutils.get_inverse_predicate_value(
                    temp_id, 'to:templatizedId', level=1)
            if len(stored_id):
                tofler_id = stored_id[0]
                self._templatized_id_map[temp_id] = tofler_id
            else:
                tofler_id = Common.generate_id(None)
                self._templatized_id_map[temp_id] = tofler_id
                self._new_templatized_id.append(temp_id)
        return self._templatized_id_map[temp_id]

    def handle_templatized_id(self, elem, additional_check=None,
                              additional_check_args=None):
        '''
        given a templatized id the method will return tofler_id
        after caching the tofler_id to templatized_id map for future reference
        templatized_id rule:
            if a id starts with _(underscore), it is a templatized_id
            if a id starts with __(double underscore), it is a throw away id
        '''

        if templatizedid.is_templatized_id(elem):
            if additional_check is None or \
                    additional_check(
                        elem, additional_check_args=additional_check_args):
                temp_id = templatizedid.append_userid(elem, self._author)
                tofler_id = self.add_templatized_id_map(temp_id)
                elem = tofler_id

        return elem

    def is_templatized_object(self, elem, additional_check_args):
        pred = additional_check_args[1]
        if pred == 'to:templatizedId':
            return False
        (pred_subclass, pred_type_subclass) = self.gather_info_from_toflerdb(
            pred, ['subclass', 'type_subclass'])
        if collection.intersection(
                ['to:RelationalProperty', 'to:ComplexRelationalProperty'],
                pred_subclass + pred_type_subclass):
            return True
        return False

    def add_input(self, subj, pred, objc, prev=None):
        self._fact_tuple = (subj, pred, objc)
        subj = self.handle_templatized_id(subj)
        pred = self.handle_templatized_id(pred)
        objc = self.handle_templatized_id(
            objc, additional_check=self.is_templatized_object,
            additional_check_args=(subj, pred, objc))
        if self._ignore_duplicate and \
                eternity_dbutils.tuple_exists_in_eternity(subj, pred, objc):
            return self

        self.normalize_input(subj, pred, objc)
        if self._validation and not self.is_valid(subj, pred, objc):
            self.delete_normalize_input(subj, pred, objc)
            return self
        self._fact_id = Common.generate_id(None)

        self.create_eternity_input(subj, pred, objc, prev)
        self.create_snapshot_input(subj, pred, objc)
        return self

    def gather_info_from_toflerdb(self, elem, prop_list):
        ret_val = []
        if elem not in self._normalized_input:
            self._normalized_input[elem] = {}
        for prop in prop_list:
            if not prop in self._normalized_input[elem]:
                if prop == 'type':
                    self._normalized_input[elem][prop] = \
                        dbutils.get_predicate_value(
                            elem, 'to:type',
                            additional_lookup=self._normalized_input)
                elif prop == 'subclass':
                    self._normalized_input[elem][prop] = \
                        dbutils.get_predicate_value(
                            elem, 'to:subClassOf',
                            additional_lookup=self._normalized_input)
                elif prop == 'domain':
                    self._normalized_input[elem][prop] = \
                        dbutils.get_predicate_value(
                            elem, 'to:domain', level=1,
                            additional_lookup=self._normalized_input)
                elif prop == 'range':
                    self._normalized_input[elem][prop] = \
                        dbutils.get_predicate_value(
                            elem, 'to:range',
                            additional_lookup=self._normalized_input)
                elif prop == 'type_subclass':
                    (prop_type, ) = self.gather_info_from_toflerdb(
                        elem, ['type'])
                    self._normalized_input[elem][prop] = \
                        dbutils.get_predicate_value(
                            prop_type, 'to:subClassOf',
                            additional_lookup=self._normalized_input)
                elif prop == 'type_domain':
                    (prop_type, ) = self.gather_info_from_toflerdb(
                        elem, ['type'])
                    self._normalized_input[elem][prop] = \
                        dbutils.get_predicate_value(
                            prop_type, 'to:domain', level=1,
                            additional_lookup=self._normalized_input)
                elif prop == 'type_range':
                    (prop_type, ) = self.gather_info_from_toflerdb(
                        elem, ['type'])
                    self._normalized_input[elem][prop] = \
                        dbutils.get_predicate_value(
                            prop_type, 'to:range',
                            additional_lookup=self._normalized_input)
            ret_val.append(self._normalized_input[elem][prop])

        return tuple(ret_val)

    def is_valid(self, subj, pred, objc):
        subj_type, subj_type_subclass = self.gather_info_from_toflerdb(
            subj, ['type', 'type_subclass'])
        pred_type, pred_subclass, pred_type_subclass = \
            self.gather_info_from_toflerdb(
                pred, ['type', 'subclass', 'type_subclass'])

        # if subj_type must not be empty
        if not subj_type:
            error_txt = (
                'Subject type must be defined : %s\nInput Tuple : %s'
                % (subj, self._fact_tuple))
            # Common.get_logger().error(error_txt)
            raise exceptions.InvalidInputValueError(error_txt)
            return False

        if not 'to:Property' in pred_type_subclass + pred_subclass:
            error_txt = (
                'Predicate root type must be of to:Property : %s'
                '\nInput Tuple : %s' % (pred, self._fact_tuple))
            # Common.get_logger().error(error_txt)
            raise exceptions.InvalidInputValueError(error_txt)
            return False

        # if pred is 'to:type', we need to special handle the case
        # check whether the object exists in ontology
        if pred == 'to:type' and not dbutils.exists_in_eternity(
                objc, ontology_only=True):
            error_txt = (
                'The type does not exist in ontology: %s\nInput Tuple : %s'
                % (objc, self._fact_tuple))
            # Common.get_logger().error(error_txt)
            raise exceptions.InvalidInputValueError(error_txt)
            return False

        # if pred isA to:RelationalProperty, conditions to satisfy
        # 1. object needs to exists in toflerdb
        # 2. pred_domain should be in subj_type + subject_type_subclass
        # 3. pred_range should be in objc_type + objc_type_subclass
        elif 'to:RelationalProperty' in pred_subclass:
            if not dbutils.exists_in_eternity(
                    objc, additional_lookup=self._normalized_input):
                error_txt = (
                    'Object does not exists in toflerdb : %s\nInput Tuple : %s'
                    % (objc, self._fact_tuple))
                # Common.get_logger().error(error_txt)
                raise exceptions.InvalidInputValueError(error_txt)
                return False
            (pred_domain, pred_range) = self.gather_info_from_toflerdb(
                pred, ['domain', 'range'])
            if not collection.intersection(
                    pred_domain, subj_type + subj_type_subclass):
                error_txt = (
                    'Predicate domain does not satisfy ontology : %s'
                    '\nInput Tuple : %s' % (pred, self._fact_tuple))
                # Common.get_logger().error(error_txt)
                raise exceptions.InvalidInputValueError(error_txt)
                return False
            (objc_type, objc_type_subclass) = self.gather_info_from_toflerdb(
                objc, ['type', 'type_subclass'])
            if not collection.intersection(
                    pred_range, objc_type + objc_type_subclass):
                error_txt = (
                    'Predicate range does not satisfy ontology : %s'
                    '\nInput Tuple : %s' % (pred, self._fact_tuple))
                # Common.get_logger().error(error_txt)
                raise exceptions.InvalidInputValueError(error_txt)
                return False

        # if pred isA to:ComplexRelationalProperty, conditions to satisfy
        # 1. object needs to exists in toflerdb
        # 2. pred_type_domain should be in subj_type + subject_type_subclass
        # 3. pred_type_range should be in objc_type + objc_type_subclass
        elif 'to:ComplexRelationalProperty' in pred_type_subclass:
            if not dbutils.exists_in_eternity(
                    objc, additional_lookup=self._normalized_input):
                error_txt = (
                    'Object does not exists in toflerdb : %s\nInput Tuple : %s'
                    % (objc, self._fact_tuple))
                # Common.get_logger().error(error_txt)
                raise exceptions.InvalidInputValueError(error_txt)
                return False
            (pred_type_domain, pred_type_range) = \
                self.gather_info_from_toflerdb(
                    pred, ['type_domain', 'type_range'])
            if not collection.intersection(
                    pred_type_domain, subj_type + subj_type_subclass):
                error_txt = (
                    'Predicate domain does not satisfy ontology : %s'
                    '\nInput Tuple : %s' % (pred, self._fact_tuple))
                # Common.get_logger().error(error_txt)
                raise exceptions.InvalidInputValueError(error_txt)
                return False
            (objc_type, objc_type_subclass) = self.gather_info_from_toflerdb(
                objc, ['type', 'type_subclass'])
            if not collection.intersection(
                    pred_type_range, objc_type + objc_type_subclass):
                error_txt = (
                    'Predicate range does not satisfy ontology : %s'
                    '\nInput Tuple : %s' % (pred, self._fact_tuple))
                # Common.get_logger().error(error_txt)
                raise exceptions.InvalidInputValueError(error_txt)
                return False

        # if pred isA to:ComplexProperty, conditions to satisfy
        # 1. pred_type_domain should be in subj_type + subject_type_subclass
        elif 'to:ComplexProperty' in pred_type_subclass:
            (pred_type_domain, ) = self.gather_info_from_toflerdb(
                pred, ['type_domain'])
            if not collection.intersection(
                    pred_type_domain, subj_type + subj_type_subclass):
                error_txt = (
                    'Predicate domain does not satisfy ontology : %s'
                    '\nInput Tuple : %s' % (pred, self._fact_tuple))
                # Common.get_logger().error(error_txt)
                raise exceptions.InvalidInputValueError(error_txt)
                return False

        # if pred isA to:Property, conditions to satisfy
        # 1. pred_domain should be in subj_type + subject_type_subclass
        else:
            (pred_domain, ) = self.gather_info_from_toflerdb(pred, ['domain'])
            if not collection.intersection(
                    pred_domain, subj_type + subj_type_subclass):
                error_txt = (
                    'Predicate domain does not satisfy ontology : %s'
                    '\nInput Tuple : %s' % (pred, self._fact_tuple))
                # Common.get_logger().error(error_txt)
                raise exceptions.InvalidInputValueError(error_txt)
                return False

        return True

    def create_eternity_input(self, subj, pred, objc, prev=None):
        eternity_value = None
        (pred_type, pred_subclass, pred_type_subclass) = \
            self.gather_info_from_toflerdb(
                pred, ['type', 'subclass', 'type_subclass'])
        if 'to:ComplexRelationalProperty' in pred_type_subclass or \
                'to:RelationalProperty' in pred_subclass:
            # the object is reference to some other entity
            pass
        else:
            eternity_value = objc
            objc = None
        (subj, pred, objc) = eternity_dbutils.convert_elements_to_string(
            subj, pred, objc)
        (subj, pred, eternity_value) = \
            eternity_dbutils.convert_elements_to_string(
                subj, pred, eternity_value)
        self._input_list.append({
            'subject': subj,
            'predicate': pred,
            'object': objc,
            'value': eternity_value,
            'fact_id': self._fact_id,
            'author': self._author,
            'prev': prev
        })

    def get_node_from_snapshot(self, subj, pred=None, objc=None):
        if pred == 'to:type':
            return
        if locks.is_locked(subj) and subj not in self._locked_nodes:
            raise exceptions.WriteLockedNodeError(
                'The node is locked for writing: %s\nInput Tuple : %s'
                % (subj, self._fact_tuple))
        (subj_type, ) = self.gather_info_from_toflerdb(subj, ['type'])
        if not collection.find_path(self._stores, subj) and \
                not collection.find_path(self._stores, subj_type[0], _id=subj):
            attempts = 2
            while attempts > 0:
                attempts -= 1
                node = snapshot_dbutils.get_snapshot_nodes(id=subj)
                if not node:
                    # Common.get_logger().error(
                    # 'Invalid id, ATTEMPTS Again : %s, %s' % (subj, pred))
                    time.sleep(1)
                else:
                    self._stores['snapshot'].update(node)
                    self._locked_nodes += locks.lock_nodes(
                        [key for key in node])
                    break

    def add_superclasses(self, elem):
        return self.gather_info_from_toflerdb(elem, ['subclass'])[0] + [elem]

    def create_snapshot_input(self, subj, pred, objc):
        (subj_type, subj_type_subclass) = self.gather_info_from_toflerdb(
            subj, ['type', 'type_subclass'])
        (pred_type, pred_type_subclass, pred_type_range) = \
            self.gather_info_from_toflerdb(
                pred, ['type', 'type_subclass', 'type_range'])

        # if pred_type_subclass isA to:ComplexProperty or
        # to:ComplexRelationalProperty, this means the complex property is
        # getting assigned to some other node or some other complex property
        # we will be assigning value key only if pred_type_range is not
        # 'to:Null'.
        # if the subj exists in _stores, this means subject is either entity in which case
        # the subj would be found in _stores['snapshot'] or
        # the subj is an instance of any ComplexRelationalProperty/ ComplexProperty,
        # in which case the subject is found in _stores['memory']
        # if the subj does not exists in _stores as key, this means the subject
        # is an instance of ComplexRelationalProperty/ ComplexProperty, in which case
        # the subject can be found under the subj_type key with subject as id
        # we need to assign the value {pred_type : objc} at the appropriate
        # place
        if collection.intersection(
                ['to:ComplexProperty', 'to:ComplexRelationalProperty'],
                pred_type_subclass):
            self.get_node_from_snapshot(subj, pred)
            if not pred in self._stores['inmemory']:
                raise exceptions.InvalidInputValueError(
                    'ComplexProperty property instance can be used only once :'
                    ' %s\nInput Tuple : %s' % (pred, self._fact_tuple))

            value = self._stores['inmemory'][pred]
            is_unique = dbutils.is_unique_predicate_value(pred_type[0])
            value.update({
                'fact_id': self._fact_id
            })
            if 'to:Null' not in pred_type_range:
                value.update({
                    'value': self.typecast_input(subj, pred, objc)
                    if 'to:type' not in pred_type
                    else self.add_superclasses(objc)
                })
            if not is_unique:
                value = [value]
            if collection.find_path(self._stores, subj):
                collection.assign_value(self._stores, subj, {
                                        pred_type[0]: value})
            else:
                collection.assign_value(self._stores, subj_type[0], {
                                        pred_type[0]: value}, _id=subj)
            del self._stores['inmemory'][pred]

        # if subj_type_subclass isA to:ComplexProperty or to:ComplexRelationalProperty
        # subject is either a key in _stores['memory'] or id value of key subj_type
        # find the subject as key or find subj_type with subject as fact_id
        # assign {pred : objc} under proper key
        elif collection.intersection(
                ['to:ComplexProperty', 'to:ComplexRelationalProperty'],
                subj_type_subclass):
            # this means we have got a complex relation.
            # get the object from elasticsearch if possible
            self.get_node_from_snapshot(subj, pred)
            is_unique = dbutils.is_unique_predicate_value(pred)
            value = {pred: self.add_fact_id(
                self.typecast_input(subj, pred, objc),
                is_unique, pred == 'to:type')}
            if collection.find_path(self._stores, subj):
                collection.assign_value(self._stores, subj, value)
            elif collection.find_path(self._stores, subj_type[0], _id=subj):
                collection.assign_value(
                    self._stores, subj_type[0], value, _id=subj)
            else:
                self._stores['inmemory'][subj] = {'id': subj}
                collection.assign_value(self._stores, subj, value)

        # if subj_type_subclass isA to:Entity, subject can be found under _stores['snapshot']
        # assign the pred value under it
        # important thing here is the use of if, elif
        # the order ensures that we don't get any complex kind of assignment at this point
        # this is really simple property assertion, and that is why we don't get any
        # "if 'to:Property' in" kind of condition check anywhere
        elif 'to:Entity' in subj_type_subclass:
            # this means we have got a node.
            # get the subject from elasticsearch if possible
            self.get_node_from_snapshot(subj, pred)
            is_unique = dbutils.is_unique_predicate_value(pred)
            value = {pred: self.add_fact_id(
                self.typecast_input(subj, pred, objc),
                is_unique, pred == 'to:type')}
            if subj not in self._stores['snapshot']:
                self._stores['snapshot'][subj] = {
                    'id': subj
                }
            collection.assign_value(self._stores, subj, value)

    def get_snapshot(self):
        return self._stores['snapshot']

    def add_templatized_id_to_insert(self):
        prev_validation = self._validation
        self.turn_off_validation()
        for temp_id in self._new_templatized_id:
            if templatizedid.is_keep_id(temp_id):
                tofler_id = self._templatized_id_map[temp_id]
                self.add_input(tofler_id, 'to:templatizedId', temp_id)
        self._validation = prev_validation

    def commit(self):
        # create entries for any new storable templatized ids
        # we needed to call it seperately than handle_templatized_id,
        # as we want to call add_input(tofler_id, 'to:templatizedId', temp_id
        # once we have completed replacing temp_id with tofler_id
        try:
            self.add_templatized_id_to_insert()
            # self.add_superclasses_for_type_field(self._stores['snapshot'])
            # commit both the query list
            snapshot_dbutils.insert_into_snapshot(self._stores['snapshot'])
            eternity_dbutils.insert_into_eternity(self._input_list)
        finally:
            self.clean_up()

    def clean_up(self):
        self._input_list = []
        self._stores['snapshot'] = {}
        self._normalized_input = {}
        self._new_templatized_id = []
        locks.release_lock(self._locked_nodes)
        self._locked_nodes = []
