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
    
	@Override
	protected void postSave() throws DaoException {
		((Resource) resource).updateTagsAndIncrements(id, tagAdds, tagRemoves, fieldIncrements);
		
		super.postSave();
	}
}
