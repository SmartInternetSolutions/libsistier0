package com.sis.dao.couchdb;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.jcouchdb.db.Database;
import org.jcouchdb.db.Options;
import org.jcouchdb.document.ValueRow;
import org.jcouchdb.document.ViewResult;
import org.jcouchdb.exception.NotFoundException;
import org.svenson.JSON;

import com.sis.sds.core.Config;
import com.sis.sds.core.SDS;
import com.sis.util.dao.Collection;
import com.sis.util.dao.Exception;
import com.sis.util.dao.Object;

/**
 * 
 * @author CR
 *
 * IMPORTANT: this is not an abstract way to implement couchdb. this is especially designed for SDS.
 *
 */
class Resource implements com.sis.dao.Resource {
	protected String idFieldName 	= "_id";
	protected String tableName		= "";
	
//	public Resource() {
//		
//	}
	
    public String getIdFieldName() {
		return idFieldName;
	}

	public void setIdFieldName(String idFieldName) {
		this.idFieldName = idFieldName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	static protected Database connection = null;

	static protected Database createConnection(String name) {
		Database db = new Database(
			Config.getSingleton().getValue("couchdb/" + name + "/address"), 
			Config.getSingleton().getValue("couchdb/" + name + "/database")
		);
		
		return db;
	}
	
	static protected Database getConnection(String name) {
		if (connection == null) {
			connection = createConnection(name);
		}
		
		return connection;
	}
	
	static protected Database getReadConnection() {
		return getConnection("slave");
	}
	
	static protected Database getWriteConnection() {
		return getConnection("master");
	}
	
	@Override
	public int getCapabilities() {
		return 	CAPABILITY_UPDATE |
				CAPABILITY_DELETE |
				CAPABILITY_PRIMARYKEY |
				CAPABILITY_INSERT;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void load(Object object, String value, String field)
			throws Exception {
		
		try {
			if (field == idFieldName) {
				HashMap<String, Object> document = getReadConnection().getDocument(HashMap.class, value);
				
				for (String key : document.keySet()) {
					object.setData(key, document.get(key));
				}
			} else {
				// TODO
			}
		} catch (NotFoundException e) {
			throw new Exception("item not found");
		}
		
//		DBCollection dbColl = getReadConnection().getCollection(tableName);
//		
//		BasicDBObject bdbo = new BasicDBObject();
//		
//		if (field.equalsIgnoreCase(idFieldName)) {
//			bdbo.append(idFieldName, new ObjectId(value));
//		} else {
//			bdbo.append(field, value);
//		}
//				
//		DBObject doc = dbColl.findOne(bdbo);
//		
//		if (doc == null) {
//			throw new Exception("entry not found");
//		}
//		
//		for (String key : doc.keySet()) {
//			object.setData(key, doc.get(key));
//		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void loadCollection(Collection collection, java.lang.Object[] args)
			throws Exception {
		// TODO Auto-generated method stub
		String tableName = (String) args[0];
		String viewName = (String) args[1];
		Integer limit = (Integer) args[2];
		Integer offset = (Integer) args[3];
		
//		String uri = "/_design/" + tableName + "/_view/" + viewName;
		
		Options opts = new Options();
		
		if (limit > 0) {
			opts.limit(limit);
		}
		
		if (offset > 0) {
			opts.skip(offset);
		}
		
//		HashMap<String, java.lang.Object> keys = new HashMap<String, java.lang.Object>(); 
//		keys.put("startkey", "[1]");
//		keys.put("endkey", "[1,[]]");
		ViewResult<java.lang.Object> result = getReadConnection().queryView(tableName + "/" + viewName, java.lang.Object.class, opts, null);
//			getReadConnection().query(uri, java.lang.Object.class, opts, null, keys);
//			getReadConnection().query(uri + "?startkey=[1]&endkey=[1,[]]", java.lang.Object.class, opts, null, null);
		//ByKeys(args[0].toString(), java.lang.Object.class, null, opts, null);
		
		for (ValueRow<java.lang.Object> object : result.getRows()) {
			Object item = collection.newItem();
			
			item.setId(object.getId());
			item.setData((Map<String, java.lang.Object>) object.getValue());
			
			collection.addItem(item);
		}
	}

	@Override
	public String insert(Map<String, java.lang.Object> fields) throws Exception {
		String id = UUID.randomUUID().toString(); // TODO: use global id generator
		fields.put(idFieldName, id);
		fields.put("type", tableName);
		
		getWriteConnection().createDocument(fields);
		
		return id;
	}

	@Override
	public boolean update(String id, Map<String, java.lang.Object> updateMap)
			throws Exception {
		HashMap<String, java.lang.Object> map = new HashMap<String, java.lang.Object>();
		map.putAll(updateMap);
		
		// in-place has to be defined (see Setup)
		try {
			String name = getWriteConnection().getName();
			getWriteConnection().getServer().post("/" + name + "/_design/app/_update/in-place/" + id, JSON.defaultJSON().forValue(map)).getContent();
		} catch (java.lang.Exception e) {
			SDS.addLog("sis.sds.dao.couchdb", e.getClass().getName() + ": " + e.getMessage());
			return false;
		}
				
		return true;
	}

	/**
	 * support for atomic add/remove of tags
	 * 
	 * @param id
	 * @param tagsToAdd
	 * @param tagsToRemove
	 * @return
	 * @throws Exception
	 */
	public boolean updateTags(String id, Map<String, Set<String>> tagsToAdd, Map<String, Set<String>> tagsToRemove)
		throws Exception {
		HashMap<String, java.lang.Object> map = new HashMap<String, java.lang.Object>();
		map.put("addTags", tagsToAdd);
		map.put("removeTags", tagsToRemove);

		// manage-tags has to be defined (see Setup)
		try {
			String name = getWriteConnection().getName();
			getWriteConnection().getServer().post("/" + name + "/_design/app/_update/manage-tags/" + id, JSON.defaultJSON().forValue(map)).getContent();
		} catch (java.lang.Exception e) {
			SDS.addLog("sis.sds.dao.couchdb", e.getClass().getName() + ": " + e.getMessage());
			return false;
		}
		
		return true;
	}
	
	@Override
	public boolean delete(String id) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void beginTransaction() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void commit() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void rollback() throws Exception {
		// TODO Auto-generated method stub

	}

}
