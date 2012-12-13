package com.sis.dao.mongodb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBList;
import com.sis.dao.DaoException;

/**
 * @author CR
 */
// TODO: abstract addTag, removeTag and incrementField. they are useful for other adapters as well
public class DataObject extends com.sis.dao.DataObject {
	protected void initResource() {
		Resource res = new Resource();
		res.setTableName(tableName);
		res.setIdFieldName(idFieldName);
		
		resource = res;
	}
	
	public DataObject() {
		super();
		
		idFieldName = "_id";
		
		initResource();
	}

	private static Object cleanObject(Object o) {
		java.lang.Object obj = o; 
    	
    	if (obj instanceof BasicDBList) {
    		return ((BasicDBList) obj).toArray();
    	}
    	
    	return obj;
	}
	
    public java.lang.Object getData(String key) {
    	if (!hasData(key)) {
    		return null;
    	}
    	
    	return cleanObject(data.get(key));
    }
    
	private final Map<String, Set<String>> tagAdds 		= new HashMap<String, Set<String>>();
	private final Map<String, Set<String>> tagRemoves	= new HashMap<String, Set<String>>();
	
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
	
	private final Map<String, Integer> fieldIncrements = new HashMap<String, Integer>();

	protected void incrementField(String field, int delta) {
		fieldIncrements.put(field, delta);
	}
	
	@Override
	protected void postSave() throws DaoException {
		((Resource) resource).updateTagsAndIncrements(id, tagAdds, tagRemoves, fieldIncrements);
		
		super.postSave();
	}
}
