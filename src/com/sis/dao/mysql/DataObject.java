package com.sis.dao.mysql;

import java.util.Map;
import java.util.Map.Entry;

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
	
	@Override
	protected Map<String, Object> createInsertMap() {
		Map<String, Object> map = super.createInsertMap();
		
		if (!fieldIncrements.isEmpty()) {
			for (Entry<String, Integer> field : fieldIncrements.entrySet()) {
				map.put(field.getKey(), field.getValue());
			}
		}
		
		return map;
	}
	
	@Override
	protected Map<String, Object> createUpdateMap() {
		Map<String, Object> map = super.createUpdateMap();
		
		if (!fieldIncrements.isEmpty()) {
			for (Entry<String, Integer> field : fieldIncrements.entrySet()) {
				map.put(field.getKey(), new DbExpression("`" + field.getKey() + "` + (" + field.getValue() + ")"));
			}
		}
		
		return map;
	}
}
