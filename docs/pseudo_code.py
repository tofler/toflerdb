steps to insert into eternity:
inputs:
	list of(
		subject		t1,
		predicate	p1,
		object 		o1,
		author 		= tofler_bot
		prev		= None
	)

========== validity checks ==========
	# subject basic
	if t1 exists in eternity
		t1_type = get_attribute(t1, 'rdf:type', root = True)
		if not t1_type is rdfs:Class
			return # REJECT
	else:
		new_entity = True
		t1_type = find type from inputs
	if t1_type is None:
		return # REJECT
	# predicate basic
	if p1 exists in ontology
		p1_tyoe = get_attribute(p1, 'rdf:type', root = True)
		if not p1_type is rdfs:Property
			return # REJECT
	else:
		return # REJECT

	# predicate domain and range
	p1_type = get_attribute(p1, 'rdf:type')			# to:subsidiaryOf
	p1_domain = get_attribute(p1, 'rdfs:domain')	# to:Business
	p1_range = get_attribute(p1, 'rdfs:range')		# to:Business

	t1_types = a list of types fetched from toflerdb_snapshot(elastic)	# [to:Business, to:IndianCompany]
	if not p1_domain in t1_types:
		return # REJECT
	
	# object basic
	if object exists in eternity:
		object goes to object column
		o1_types = a list of types fetched from toflerdb_snapshot(elastic)	# [to:Business, to:IndianCompany]
		if not p1_range in o1_types:
			return # REJECT

	else:
		object goes to value column
		p1_range_type = get_attribute(t1, 'rdf:type', root = True)
		if not p1_range_type is 'rdfs:Literal':
			return # REJECT

========= validity check ends ==========

At this point we have all valid inputs t1, p1, o1
insert inot toflerdb_eternity(t1, p1, o1, author, prev)
if prev is None:
	# this is an insert
	call insert_into_toflerdb_snapshot(t1, p1, o1, author, prev)
else:
	# this is an update
	call update_toflerdb_snapshot(t1, p1, o1, author, prev)





insert_into_toflerdb_snapshot(t1, p1, o1, author, prev):
	node = get_node(t1)



======== Snapshot maker =======
self.stores['snapshot'] = {}
self._inmemory_objects = {}
if subj not in self.stores['snapshot']:
	# find the subj in snapshot world
	node = dbutils.get_snapshot_node(subj)
	if node is not None:
		self.stores['snapshot'][node['id']] = node

if input_info['input']['value']:
	# it means, current input has a literal value 
	# subject might be found either in self.stores['snapshot']
	# or self._inmemory_objects
	if pred in self._stores['_inmemory']:
		node_key = pred_type[0] # direct predicate type
		value = {
			'value' : objc,
		}.update(self._stores['_inmemory'][pred])
		del self._stores['_inmemory'][pred]
		path = find_path(subj)
		assign_value(value, path)
	if dbutils.exists(pred, eternity_only = True):
		# this means some complex property is having another attribute
		# we need to find the proper path 
	else:
		path = find_path(subj)
		assign_value(pred, objc, path)

if input_info['input']['object']:
	# it means, current input is either an entity, relation or complex attribute
	if dbutils.exists(subj, eternity_only = True) and dbutils.exists(objc, eternity_only = True)
		path = find_path(pred)
		if not path:
			raise error
		else:
			value = {
				'v1' : subj,
				'v2' : objc
			}
			assign_value(pred, value, path)
	if 'rdfs:Class' or 'rdfs:Property' in objc_type:
		# entity
		if subj in self.stores['snapshot']:
			path = 
			assign_value(pred, objc, path)
		else:
			create_new
	if 'rdfs:Literal' in objc_type:
		# complex Property


================== onto maker ================
if pred == 'rdf:type' and objc == 'rdfs:Property':
	self._stores['inmemory'].update({
		pred_node : {
			subj : {
				'properties' : {
					'fact_id' : {
						'type' : 'int'
					}
				}
			}
		}
	})
if pred == 'rdfs:domain':
	objc_type = dbutils.get_predicate_value(objc, 'rdf:type')
	if 'rdfs:Class' in objc_type:
		self._stores['snapshot'].update({
			pred_node : {
				subj : {
					'properties' : {
						'fact_id' : {
							'type' : 'int'
						}
					}
				}
			}
		})
	if 'rdfs:Property' in objc_type:
		pass
if pred == 'rdfs:range':
	if 'rdfs:Literal' in objc_type:
		self._stores['snapshot'].update({
			pred_node : {
				subj : {
					'properties' : {
						'fact_id' : {
							'type' : 'int'
						},
						'value' : {
							'type' : dbutils.get_type(objc_type)
						}
					}
				}
			}
		})





if pred == 'rdfs:domain':
	domain_type = dbutils.get_predicate_value(objc, 'rdf:type', level = 5)
	if 'rdfs:Class' in domain_type:
		# this means it is a top label thing
		self._mapping_properties.update({
			subj: {
				'fact_id' : {
					'type' : 'int'
				}
			}
		})
	if 'rdfs:Property' in domain_type:
		# this is a complex kind probably
		# (if 'rdfs:Literal' in range_type)
		# probably save it for later
		property_node = dbutils.get_property_node(objc)
		value = {
			subj: {
				'fact_id' : {
					'type' : 'int'
				}
			}
		}
		assign_value(property_node, objc, value)
if pred == 'rdfs:range':
	range_type = dbutils.get_predicate_value(objc, 'rdf:type', level = 5)
	if 'rdfs:Literal' in range_type:
		








































if 'rdfs:Property' in subj_type:
	# this means, it is going to be in the mapping,
	# now to decide whether it's directly a property or
	