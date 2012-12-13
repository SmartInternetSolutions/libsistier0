package com.sis.dao.couchdb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.sis.util.dao.Exception;

public class Object extends com.sis.dao.DataObject {
	protected void initResource() {
		Resource res = new Resource();
		res.setTableName(tableName);
		res.setIdFieldName(idFieldName);
		
		resource = res;
	}
	
	public Object() {
		super();
		
		idFieldName = "_id";
		
		initResource();
	}

	Map<String, Set<String>> tagAdds 	= new HashMap<String, Set<String>>();
	Map<String, Set<String>> tagRemoves = new HashMap<String, Set<String>>();
	
	protected void addTag(String field, String tag) {
		if (!tagAdds.containsKey(field)) {
			tagAdds.put(field, new HashSet<String> ());
		}
		
		tagAdds.get(field).add(tag);
		tagRemoves.remove(field);
	}
	
	protected void removeTag(String field, String tag) {
		if (!tagRemoves.containsKey(field)) {
			tagRemoves.put(field, new HashSet<String> ());
		}
		
		tagRemoves.get(field).add(tag);
		tagAdds.remove(field);
	}
	
	@Override
	protected void postSave() throws Exception {
		((Resource) resource).updateTags(id, tagAdds, tagRemoves);
		
		super.postSave();
	}
}
