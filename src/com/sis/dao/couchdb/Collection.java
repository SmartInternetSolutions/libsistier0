package com.sis.dao.couchdb;

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
	
	private String viewName = "alldocs";
	
	protected void loadCollection(String viewName) {
		this.viewName = viewName;
		loadCollection();
	}
	
	protected void loadCollection() {
		try {
			resource.loadCollection(this, new java.lang.Object[] {
				tableName, 
				viewName,
				(int) limitCount,
				(int) limitOffset
			});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
