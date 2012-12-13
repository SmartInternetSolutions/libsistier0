package com.sis.dao.mongodb;

public class Collection extends com.sis.dao.Collection {
	protected void initResource() {
		Resource res = new Resource();
		res.setTableName(tableName);
		res.setIdFieldName(idFieldName);
		
		resource = res;
	}
	
	public Collection() {
		super();
		
		initResource();
	}
	
	protected void loadCollection() {
		((Resource) resource).loadCollection(this, fieldFilters, limitCount, limitOffset, fieldSortings);
	}
}
