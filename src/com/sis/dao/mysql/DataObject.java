package com.sis.dao.mysql;

public class DataObject extends com.sis.dao.DataObject {
	protected void initResource() {
		Resource res = new Resource();
		res.setTableName(tableName);
		res.setIdFieldName(idFieldName);

		resource = res;
	}

//	protected void setShardedBy(String fieldName) {
//		((Resource) resource).setShardedBy(fieldName);
//	}

	public DataObject() {
		super();

		initResource();
	}
}
