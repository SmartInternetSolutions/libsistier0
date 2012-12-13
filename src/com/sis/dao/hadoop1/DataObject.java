package com.sis.dao.hadoop1;

import com.sis.dao.DaoException;

/**
 * you can ONLY load and insert, not modify!
 * 
 * @author CR
 */
public class DataObject extends com.sis.dao.DataObject {
	public DataObject() {
		super();
	
		initResource();
	}
	
	protected void initResource() {
		Resource res = new Resource();
//		res.setTableName(tableName);
//		res.setIdFieldName(idFieldName);
		
		res.setCluster(idFieldName);
		res.setGroup(tableName);
		
		resource = res;
	}
	
	@Override
	public void load(String value, String field) throws DaoException {
		throw new DaoException("load not supported");
	}
}
