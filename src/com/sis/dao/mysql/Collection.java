package com.sis.dao.mysql;

import com.sis.dao.DaoException;

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

	protected void addJoinStatement(String tablename, String expression) {
		((Resource) resource).addJoinStatement(tablename, expression);
	}
	
	protected void addLeftJoinStatement(String tablename, String expression) {
		((Resource) resource).addLeftJoinStatement(tablename, expression);
	}

	protected void addJoinStatement(String tablename, String expression, String columns[]) {
		((Resource) resource).addJoinStatement(tablename, expression, columns);
	}
	
	protected void addLeftJoinStatement(String tablename, String expression, String columns[]) {
		((Resource) resource).addLeftJoinStatement(tablename, expression, columns);
	}


	@Override
	protected void loadCollection() {
		try {
			((Resource) resource).loadCollection(this, fieldFilters, limitCount, limitOffset, fieldSortings);
		} catch (DaoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public long getCount() {
		return ((Resource) resource).count(fieldFilters, limitCount, limitOffset);
	}
}
