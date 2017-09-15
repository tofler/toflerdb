from toflerdb.utils import exceptions
from toflerdb.utils.common import Common
from toflerdb.dbutils import dbutils
from toflerdb.utils import collection


class OntoMaker(object):

    def __init__(self):
        self._sql_query_list = []
        self._elastic_query_list = []
        self._entity_id = None
        self._entity_type_list = None
        self._input_list = []
        self._normalized_input = {}
        self._inverse_normalized_input = {}
        self._stores = {
            'snapshot': {},
            'inmemory': {}
        }
        self._new_mapping = {}
        self._complete_mapping = dbutils.get_complete_snapshot_mapping()
        self._validation = True

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

    def inverse_normalize_input(self, subj, pred, objc):
        if subj not in self._inverse_normalized_input:
            self._inverse_normalized_input[subj] = {}
        if pred not in self._inverse_normalized_input[subj]:
            self._inverse_normalized_input[subj][pred] = []
        if objc not in self._inverse_normalized_input[subj][pred]:
            self._inverse_normalized_input[subj][pred].append(objc)

    def delete_normalize_input(self, subj, pred, objc):
        self._normalized_input[subj][pred].remove(objc)

    def add_input(self, subj, pred, objc, prev=None, author=None):
        self.normalize_input(subj, pred, objc)
        self.inverse_normalize_input(objc, pred, subj)
        if not dbutils.exists_in_ontology(subj, pred, objc):
            if self._validation:
                self.validate(subj, pred, objc)

            self._input_list.append({
                'subject': subj,
                'predicate': pred,
                'object': objc,
                'fact_id': Common.generate_id(None)
            })
        return self

    def validate(self, subj, pred, objc):
        '''
        if pred == to:subClassOf:
            (objc + objc_subclass) in [to:Entity, to:Property, to:Literal]
        elif pred == to:ontoLabel:
            subj must be define i.e. subj_subclass is non empty
        elif pred == to:description:
            subj must be define i.e. subj_subclass is non empty
        elif pred == to:domain:
            objc needs to exists in db as subject
        elif pred == to:range:
            objc needs to exists in db as subject
            if subj_subclass is Relational/Complex Relational Property:
                subj_range_subclass in [to:Entity, to:Property]
            elif subj_subclass is Property:
                subj_range_subclass is [to:Literal]
        elif pred == 'to:isUnique':
            pass
        else:
            not valid
        '''
        input_tuple = (subj, pred, objc)
        (subj_subclass, ) = self.gather_info_from_toflerdb(
            subj, ['subclass'])
        if not subj_subclass:
            error_txt = (
                "Subject is not defined : %s"
                "\nInput tuple : %s") % (subj, input_tuple)
            Common.get_logger().error(error_txt)
            raise exceptions.InvalidInputValueError(error_txt)

        if pred == 'to:subClassOf':
            (objc_subclass, ) = self.gather_info_from_toflerdb(
                objc, ['subclass'])
            if not collection.intersection(
                    objc_subclass + [objc],
                    ['to:Entity', 'to:Property', 'to:Literal']):
                error_txt = (
                    "Superclass is not defined : %s"
                    "\nInput tuple : %s") % (objc, input_tuple)
                Common.get_logger().error(error_txt)
                raise exceptions.InvalidInputValueError(error_txt)

        elif pred == 'to:ontoLabel':
            pass
        elif pred == 'to:description':
            pass
        elif pred == 'to:domain':
            if not dbutils.exists_in_eternity(
                    objc, additional_lookup=self._normalized_input,
                    ontology_only=True):
                error_txt = (
                    "Domain is not defined : %s"
                    "\nInput tuple : %s") % (objc, input_tuple)
                Common.get_logger().error(error_txt)
                raise exceptions.InvalidInputValueError(error_txt)
        elif pred == 'to:range':
            if not dbutils.exists_in_eternity(
                    objc, additional_lookup=self._normalized_input,
                    ontology_only=True):
                error_txt = (
                    "Range is not defined : %s"
                    "Input tuple : %s") % (objc, input_tuple)
                Common.get_logger().error(error_txt)
                raise exceptions.InvalidInputValueError(error_txt)

            (objc_subclass, ) = self.gather_info_from_toflerdb(
                objc, ['subclass'])
            if collection.intersection(
                    [
                        'to:RelationalProperty',
                        'to:ComplexRelationalProperty'
                    ], subj_subclass):
                if not collection.intersection(
                        [objc] + objc_subclass,
                        ['to:Entity', 'to:Property']):
                    error_txt = (
                        "For relational property declaration, range must"
                        "be either of type to:Entity or to:Property."
                        "\nProperty : %s, range type: %s."
                        "\nInput tuple : %s"
                    ) % (subj, objc_subclass, input_tuple)
                    Common.get_logger().error(error_txt)
                    raise exceptions.InvalidInputValueError(error_txt)
            elif 'to:Property' in subj_subclass:
                if not 'to:Literal' in objc_subclass:
                    error_txt = (
                        "For property declaration,"
                        "range must be of type to:Literal."
                        "\nProperty : %s, range type: %s."
                        "\nInput tuple : %s"
                    ) % (subj, objc_subclass, input_tuple)
                    Common.get_logger().error(error_txt)
                    raise exceptions.InvalidInputValueError(error_txt)
        elif pred == 'to:isUnique':
            pass
        else:
            error_txt = (
                "Predicate must be one of to:subClassOf, to:ontoLabel,"
                " to:description, to:domain, to:domain, to:range."
                "\nInput Tuple : %s") % str(input_tuple)
            Common.get_logger().error(error_txt)
            raise exceptions.InvalidInputValueError(error_txt)

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
                elif prop == 'domain_subclass':
                    (prop_domain, ) = self.gather_info_from_toflerdb(
                        elem, ['domain'])
                    self._normalized_input[elem][prop] = \
                        dbutils.get_predicate_value(
                            prop_domain, 'to:subClassOf',
                            additional_lookup=self._normalized_input)
                elif prop == 'range_subclass':
                    (prop_range, ) = self.gather_info_from_toflerdb(
                        elem, ['range'])
                    self._normalized_input[elem][prop] = \
                        dbutils.get_predicate_value(
                            prop_range, 'to:subClassOf',
                            additional_lookup=self._normalized_input)

            ret_val.append(self._normalized_input[elem][prop])

        return tuple(ret_val)

    def make_mapping(self):
        all_subjects = []
        for info in self._input_list:
            if info['subject'] not in all_subjects:
                all_subjects.append(info['subject'])

        for subj in all_subjects:
            # subj = info['subject']
            # pred = info['predicate']
            # objc = info['object']
            (
                subj_subclass,
                subj_domain,
                subj_domain_subclass,
                subj_range,
                subj_range_subclass
            ) = self.gather_info_from_toflerdb(subj, [
                'subclass', 'domain', 'domain_subclass',
                'range', 'range_subclass'])
            # print 'subj : %s\nsubj_subclass : %s\nsubj_domain :
            # %s\nsubj_domain_subclass : %s\nsubj_range :
            # %s\nsubj_range_subclass : %s' %(subj, subj_subclass, subj_domain,
            # subj_domain_subclass, subj_range, subj_range_subclass)
            if 'to:Property' not in subj_subclass:
                continue
            if 'to:Property' in subj_domain_subclass:
                for dmn in subj_domain:
                    path = collection.find_path(self._complete_mapping, dmn)
                    if path is None:
                        continue
                    path.append('properties')
                    value = {
                        subj: {
                            'properties': {
                                'fact_id': {
                                    'type': 'string',
                                    'index': 'not_analyzed'
                                },
                            }
                        }
                    }
                    if collection.intersection(
                            [
                                'to:ComplexProperty',
                                'to:ComplexRelationalProperty'
                            ], subj_subclass):
                        value[subj]['type'] = 'nested'
                    
                    if 'to:Null' in subj_range_subclass:
                        # here we don't want any value key
                        pass
                    elif collection.intersection(
                            [
                                'to:RelationalProperty',
                                'to:ComplexRelationalProperty'
                            ], subj_subclass):
                        # the value would be a reference to another node
                        value[subj]['properties']['value'] = {
                            'type': 'string', 'index': 'not_analyzed'}
                    else:
                        value[subj]['properties']['value'] = \
                            collection.get_datatype(
                                subj_range + subj_range_subclass)
                    collection.assign_value_to_path(
                        self._complete_mapping, value, path)
                    collection.assign_value_to_path(
                        self._new_mapping, value, path)
            elif 'to:Entity' in subj_domain + subj_domain_subclass:
                value = {
                    subj: {
                        'properties': {
                            'fact_id': {
                                'type': 'string',
                                'index': 'not_analyzed'
                            },
                        }
                    }
                }
                if collection.intersection(
                        [
                            'to:ComplexProperty',
                            'to:ComplexRelationalProperty'
                        ], subj_subclass):
                    value[subj]['type'] = 'nested'

                if 'to:Null' in subj_range_subclass:
                    # here we don't want any value key
                    pass
                elif collection.intersection(
                        [
                            'to:RelationalProperty',
                            'to:ComplexRelationalProperty'
                        ], subj_subclass):
                    # the value would be a reference to another node
                    value[subj]['properties']['value'] = {
                        'type': 'string',
                        'index': 'not_analyzed'
                    }
                else:
                    value[subj]['properties']['value'] = \
                        collection.get_datatype(
                            subj_range + subj_range_subclass)
                self._complete_mapping.update(value)
                self._new_mapping.update(value)
            # add all the superclass properties also
            if len(subj_subclass):
                subj_subclass_domain_of = dbutils.get_inverse_predicate_value(
                    subj_subclass, 'to:domain', level=1,
                    additional_lookup=self._inverse_normalized_input)
                # print 'subj_subclass_domain_of : %s\n\n'
                # %subj_subclass_domain_of
                for ssdo in subj_subclass_domain_of:
                    path = collection.find_path(self._complete_mapping, subj)
                    if not path:
                        continue
                    path.append('properties')
                    (ssdo_range, ) = self.gather_info_from_toflerdb(
                        ssdo, ['range'])
                    value = {
                        ssdo: {
                            'properties': {
                                'fact_id': {
                                    'type': 'string',
                                    'index': 'not_analyzed'
                                },
                                'value': collection.get_datatype(ssdo_range)
                            }
                        }
                    }
                    collection.assign_value_to_path(
                        self._new_mapping, value, path)
                    collection.assign_value_to_path(
                        self._complete_mapping, value, path)

    def make_ontology_inputs(self):
        for info in self._input_list:
            if not dbutils.exists_in_eternity(info['object']):
                info['value'] = info['object']
                info['object'] = None
            else:
                info['value'] = None

    def commit(self):
        self.make_ontology_inputs()
        dbutils.insert_into_ontology(self._input_list)
        if self._new_mapping:
            dbutils.create_snapshot_mapping({'properties': self._new_mapping})
