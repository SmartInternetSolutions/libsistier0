package com.sis.dao.mongodb;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.sis.dao.Collection;
import com.sis.dao.DaoException;
import com.sis.dao.DaoManager;
import com.sis.dao.DataObject;
import com.sis.system.Profiler;
import com.sis.system.XmlConfig;
import com.sis.util.Pair;

public class Resource implements com.sis.dao.Resource {
	private static final transient Logger logger = Logger.getLogger("MongoDB");
	
	protected String idFieldName 	= "_id";
	protected String tableName		= "";

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

	protected static transient Pair<Mongo, DB> connection = null;

	protected static synchronized Pair<Mongo, DB> createConnection(String name) {
		try {
			Mongo mongo = new Mongo(
					XmlConfig.getSingleton().getValue("mongodb/" + name + "/address"),
				Short.parseShort(XmlConfig.getSingleton().getValue("mongodb/" + name + "/port"))
			);

			DB db = mongo.getDB(XmlConfig.getSingleton().getValue("mongodb/" + name + "/database"));

			String username = XmlConfig.getSingleton().getValue("mongodb/" + name + "/username");

			if (username != null && username.length() > 0) {
				db.authenticate(username, XmlConfig.getSingleton().getValue("mongodb/" + name + "/password").toCharArray());
			}
			
			logger.debug("created connection for " + name + " to " + 
					XmlConfig.getSingleton().getValue("mongodb/" + name + "/address") + ":" + XmlConfig.getSingleton().getValue("mongodb/" + name + "/port"));

			return new Pair<Mongo, DB>(mongo, db);
		} catch (NumberFormatException e) {
			logger.error("format error!", e);
		} catch (UnknownHostException e) {
			logger.error("unknown host error!", e);
		} catch (MongoException e) {
			logger.error("unknown mongo error!", e);
		}

		return null;
	}

	protected static DB getConnection(String name) {
		if (connection == null) {
			connection = createConnection(name);
		}

		return connection.getRight();
	}

	protected static DB getReadConnection() {
		return getConnection("uni");
	}

	protected static DB getWriteConnection() {
		return getConnection("uni");
	}

	@Override
	public int getCapabilities() {
		return 	CAPABILITY_UPDATE |
				CAPABILITY_DELETE |
				CAPABILITY_PRIMARYKEY |
				CAPABILITY_NEED_FULL_UPDATE |
				CAPABILITY_INSERT;
	}

	@Override
	public void load(DataObject object, String value, String field) throws DaoException {
		Profiler profiler = new Profiler("MongoDB load Document", DaoManager.getDaoExecutionWarningThreshold());
		profiler.start();
		
		try {
			DBCollection dbColl = getReadConnection().getCollection(tableName);
	
			BasicDBObject bdbo = new BasicDBObject();
	
			if (field.equalsIgnoreCase(idFieldName)) {
				bdbo.append(idFieldName, new ObjectId(value));
			} else {
				bdbo.append(field, value);
			}
	
			DBObject doc = dbColl.findOne(bdbo);
	
			if (doc == null) {
				throw new DaoException("entry not found");
			}
	
			for (String key : doc.keySet()) {
				object.setData(key, doc.get(key));
			}
		} finally {
			profiler.stop();
		}
	}

	@Override
	public void loadCollection(Collection collection, java.lang.Object[] args) throws DaoException {
	}

	@Override
	public String insert(Map<String, java.lang.Object> fields) throws DaoException {
		Profiler profiler = new Profiler("MongoDB insert Document", DaoManager.getDaoExecutionWarningThreshold());
		profiler.start();
		
		try {
			DBObject doc = new BasicDBObject();
	
			doc.putAll(fields);
	
			getWriteConnection().getCollection(tableName).insert(doc);
			
			ObjectId lastInsertId = (ObjectId) doc.get(idFieldName);
			
			return lastInsertId.toString();
		} finally {
			profiler.stop();
		}
	}

	@Override
	public boolean update(String id, Map<String, java.lang.Object> updateMap) throws DaoException {
		Profiler profiler = new Profiler("MongoDB update Document", DaoManager.getDaoExecutionWarningThreshold());
		profiler.start();

		try {
			DBObject doc = new BasicDBObject();
	
			doc.putAll(updateMap);
	
			getWriteConnection().getCollection(tableName).update(new BasicDBObject(idFieldName, new ObjectId(id)), new BasicDBObject("$set", doc));
	
			return true;
		} finally {
			profiler.stop();
		}
	}

	@Override
	public boolean delete(String id) throws DaoException {
		Profiler profiler = new Profiler("MongoDB delete Document", DaoManager.getDaoExecutionWarningThreshold());
		profiler.start();
		
		try {
			getWriteConnection().getCollection(tableName).remove(new BasicDBObject(idFieldName, new ObjectId(id)));
		} finally {
			profiler.stop();
		}

		return true;
	}

	@Override
	public void beginTransaction() throws DaoException {
		// TODO Auto-generated method stub

	}

	@Override
	public void commit() throws DaoException {
		// TODO Auto-generated method stub

	}

	@Override
	public void rollback() throws DaoException {
		// TODO Auto-generated method stub

	}

	// @see http://www.mongodb.org/display/DOCS/Atomic+Operations
	@SuppressWarnings("serial")
	public void updateTagsAndIncrements(String id, final Map<String, Set<String>> tagAdds, final Map<String, Set<String>> tagRemoves, final Map<String, Integer> fieldIncrements) {
		Profiler profiler = new Profiler("MongoDB update tags of Document", DaoManager.getDaoExecutionWarningThreshold());
		profiler.start();
		
		try {
			getWriteConnection().getCollection(tableName).update(new BasicDBObject(idFieldName, new ObjectId(id)), new BasicDBObject() {{
				put("$pushAll", tagAdds);
				put("$pullAll", tagRemoves);
				put("$inc", fieldIncrements);
			}});
		} finally {
			profiler.stop();
		}
	}

	@SuppressWarnings("serial")
	public void loadCollection(Collection collection, final HashMap<String, Pair<java.lang.Object[], Integer>> fieldFilters, long limitCount, long limitOffset,
			HashMap<String, Integer> fieldSortings) {
		Profiler profiler = new Profiler("MongoDB load Collection", DaoManager.getDaoExecutionWarningThreshold());
		profiler.start();
		
		try {
			DBObject bdbo = new BasicDBObject();
	
			for (final String key : fieldFilters.keySet()) {
//				switch (fieldFilters.get(key).getRight()) {
//				case Collection.FILTER_FIND_IN_SET:
//					bdbo.put(key, new BasicDBObject() {{
//						put("$in", fieldFilters.get(key).getLeft());
//					}});
//					break;
//					
//				default:
//					bdbo.put(key, fieldFilters.get(key).getLeft()[0]);
//					break;
//				}
				
				if (fieldFilters.get(key).getRight() == Collection.FILTER_CUSTOM) {
					Object ibj = fieldFilters.get(key).getLeft();
					
					bdbo.put(key, ibj);
					
					continue;
				}
				
				Object[] l = fieldFilters.get(key).getLeft();
				
				if (l.length > 1) {
					bdbo.put(key, new BasicDBObject() {{
						put("$in", fieldFilters.get(key).getLeft());
					}});
				} else {
					bdbo.put(key, fieldFilters.get(key).getLeft()[0]);
				}
			}
	
			DBCursor cursor = getReadConnection().getCollection(tableName).find(bdbo);
	
			if (limitCount > 0) {
				cursor.limit((int) limitCount);
			}
	
			if (limitOffset > 0) {
				cursor.skip((int) limitOffset);
			}

//			DBObject item = null;
//			while ((item = cursor.next()) != null) {
//				Object object = collection.newItem();
//	
//			}
			

			DBObject sort = new BasicDBObject();
			
			for (Map.Entry<String, Integer> sortpair : fieldSortings.entrySet()) {
				switch (sortpair.getValue()) {
				case Collection.SORT_ASCENDING:
					sort.put(sortpair.getKey(), 1);
					break;
					
				case Collection.SORT_DESCENDING:
					sort.put(sortpair.getKey(), -1);
					break;
				}
			}
			
			cursor.sort(sort);

			for (DBObject dbObject : cursor) {
				DataObject object = collection.newItem();
	
				for (String key : dbObject.keySet()) {
					if (key == idFieldName) {
						object.setId(dbObject.get(key).toString());
					} else {
						object.setData(key, dbObject.get(key));
					}
				}
	
				collection.addItem(object);
			}
		} finally {
			profiler.stop();
		}
	}

}
