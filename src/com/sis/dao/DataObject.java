package com.sis.dao;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.svenson.JSON;

import com.sis.system.Base;

public class DataObject {
	/**
	 * wrapper for raw database statements
	 */
	public static class DbExpression {
		private final String dbExpr;
		
		public DbExpression(String dbExpr) {
			this.dbExpr = dbExpr;
		}
		
		@Override
		public String toString() {
			return dbExpr;
		}
	}
	
	public static class AsyncCallback {
		public void success(DataObject object) {
		}

		public void failed(DataObject object) {
		}
	}
	
    protected String id				= null;

    protected String idFieldName 	= "id";

	protected transient Resource resource 	= null;

    protected String tableName						= "";
    protected Map<String, java.lang.Object> data	= null;

    protected Map<String, java.lang.Object> origData	= null;
    private boolean isModified							= false;

	protected Set<String> modTable						= null;

    private boolean isDeleted = false, isNew = false;

	protected final Map<String, Set<String>> tagAdds 		= Collections.synchronizedMap(new HashMap<String, Set<String>>());
	protected final Map<String, Set<String>> tagRemoves		= Collections.synchronizedMap(new HashMap<String, Set<String>>());
	protected final Map<String, Integer> fieldIncrements 	= Collections.synchronizedMap(new HashMap<String, Integer>());

    public DataObject() {
		data 		= Collections.synchronizedMap(new HashMap<String, Object>());
		origData	= Collections.synchronizedMap(new HashMap<String, Object>());
		modTable	= new LinkedHashSet<>();
    }

    private enum DataObjectCommand {
    	DELETE,
    	UPDATE,
    	INSERT,
    	INSERT_WITHOUT_SET_ID
    }
    
    private static final Logger logger = Logger.getLogger(DataObject.class);
    
    private static class DataObjectCommandPacket {
    	private final static AtomicInteger counter = new AtomicInteger();
    	private final int id = counter.incrementAndGet();
    	
    	private final DataObjectCommand command;
    	private Map<String, Object> data = new HashMap<>();
    	private final DataObject dataObject;
		
		private int retryCounter = 0;
		
		private AsyncCallback asyncCallback = null;
    	
    	public DataObjectCommandPacket(DataObject dataObject, Map<String, Object> data, DataObjectCommand command, AsyncCallback asyncCallback) {
    		if (data != null) {
    			setData(data);
    		}
    		
    		if (asyncCallback != null) {
    			setAsyncCallback(asyncCallback);
    		}
    		
    		this.command = command;
    		this.dataObject = dataObject;
    		
    		counter.incrementAndGet();
		}
		
		public int getRetryCounter() {
			return retryCounter;
		}
		
		public int increaseRetryCounter() {
			return ++retryCounter;
		}
		
		public void setRetryCounter(int retryCounter) {
			this.retryCounter = retryCounter;
		}
		
		public void resetRetryCounter() {
			this.retryCounter = 0;
		}

		public DataObjectCommand getCommand() {
			return command;
		}

		public Map<String, Object> getData() {
			return data;
		}

		public void setData(Map<String, Object> data) {
			this.data.putAll(data);
		}

		/**
		 * @return the asyncCallback
		 */
		public AsyncCallback getAsyncCallback() {
			return asyncCallback;
		}

		/**
		 * @param asyncCallback the asyncCallback to set
		 */
		public void setAsyncCallback(AsyncCallback asyncCallback) {
			this.asyncCallback = asyncCallback;
		}

		public DataObject getDataObject() {
			return dataObject;
		}
    	
		public synchronized int getId() {
			return id;
		}
    }
    
    private static final LinkedBlockingQueue<DataObjectCommandPacket> asyncCommands = new LinkedBlockingQueue<>();
    private static final Thread asyncHandlerThread;
    
    static {
    	asyncHandlerThread = new Thread(new Runnable() {
			@Override
			public void run() {
				asyncHandlerThread.setName("DataObject Asynchronous Queue Worker");
				
				// CR: let's put through a single transaction?
				while (!Base.isShuttingDown()) {
					DataObjectCommandPacket cmd = null;
					
					try {
						while ((cmd = asyncCommands.poll(30, TimeUnit.SECONDS)) != null) {
							AsyncCallback asyncCallback = cmd.getAsyncCallback();
							DataObject dao = cmd.getDataObject();
							
							if (cmd.getRetryCounter() > 10) {
								logger.warn("stopped retrying of backlogged async command due to high error count.");
								
								if (asyncCallback != null) {
									asyncCallback.failed(dao);
								}
								continue;
							}
							
							try {
								synchronized (dao) {
									switch (cmd.getCommand()) {
									case DELETE:
										dao.getResource().delete(dao.getId());
										break;
										
									case INSERT:
										dao.setId(dao.getResource().insert(cmd.getData()));
										dao.resetStateAfterSave();
										break;
										
									case INSERT_WITHOUT_SET_ID:
										dao.getResource().insert(cmd.getData());
										dao.resetStateAfterSave();
										break;
										
									case UPDATE:
										dao.getResource().update(dao.getId(), cmd.getData());
										dao.resetStateAfterSave();
										break;
									}
								}
								
								if (asyncCallback != null) {
									asyncCallback.success(dao);
								}
							} catch(DaoException e) { // FIXME: catch failed connection exceptions here
								logger.warn("async dao exception, re-putting to queue", e);
								cmd.increaseRetryCounter();
								asyncCommands.add(cmd);
							} catch(Exception e) {
								logger.error("async command failed!", e);

								if (asyncCallback != null) {
									asyncCallback.failed(dao);
								}
							}
						}
					} catch (InterruptedException e) {
					}
				}
			}
		});
    	
    	asyncHandlerThread.setDaemon(true);
    	
    	asyncHandlerThread.start();
    }
    
    /**
     * waits until async queue is at normal size again
     */
    private static void waitForSpace() {
    	Runtime rt = Runtime.getRuntime();

    	if (asyncCommands.size() > 10000) {
    		logger.warn("async queue is getting very large, waiting for async actions to finish. freeMem = " + rt.freeMemory() + ", totalMem = " + rt.totalMemory() + ", size = " + asyncCommands.size());
    	
	    	while (asyncCommands.size() > 0) {
	    		try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
	    	}
	    	
	    	rt.gc();
    	}
    }
    
    /**
     * queues save command in a fifo backlog. no blocking at all. (errors will be ignored!)
     */
    public synchronized void saveAsync() {
    	try {
    		save(true, null);
    	} catch (DaoException e) {
    		logger.error("unexpected exception while save(true) in saveAsync()", e);
    	}
    	
    	waitForSpace();
    }
    
    /**
     * queues save command in a fifo backlog. no blocking at all.
     */
    public synchronized void saveAsync(AsyncCallback callback) {
    	try {
    		save(true, callback);
    	} catch (DaoException e) {
    		logger.error("unexpected exception while save(true) in saveAsync()", e);
    	}
    	
    	waitForSpace();
    }

    /**
     * queues delete command in a fifo backlog. no blocking at all.
     */
    public void deleteAsync(AsyncCallback asyncCallback) {
    	asyncCommands.add(new DataObjectCommandPacket(this, null, DataObjectCommand.DELETE, asyncCallback));
    	
    	waitForSpace();
    }
    
    /**
     * queues delete command in a fifo backlog. no blocking at all. (errors will be ignored!)
     */
    public void deleteAsync() {
    	asyncCommands.add(new DataObjectCommandPacket(this, null, DataObjectCommand.DELETE, null));
    	
    	waitForSpace();
    }
    
    protected void init(String idFieldName, Resource resourceModel) {
		this.idFieldName = idFieldName;
		resource = resourceModel;
    }

    public String getId() {
    	return id;
    }

    public void setId(long id) {
    	this.id = Long.toString(id);
    }

    public void setId(String id) {
    	this.id = id;
    }

    protected Resource getResource() {
    	return resource;
    }

    protected boolean hasResource() {
    	return resource != null;
    }

    protected boolean checkResourceForCapability(int cap) {
    	return (getResource().getCapabilities() & cap) != 0;
    }

    protected boolean preSave() throws DaoException {
    	return true;
    }

    protected void postSave() throws DaoException {
    }

    protected boolean preLoad() throws DaoException {
        return true;
    }

    protected void postLoad() throws DaoException {
    }

    protected boolean preDelete() throws DaoException {
        return true;
    }

    protected void postDelete() throws DaoException {
    }

    public void load(long id) throws DaoException {
    	load(Long.toString(id));
    }

    public void load(String value) throws DaoException {
    	load(value, this.idFieldName);
    }

    public synchronized void load(String value, String field) throws DaoException {
		if (preLoad()) {
			if (hasResource()) {
				getResource().load(this, value, field);
			}

		    isModified = false;
		    isDeleted = false;
		    modTable.clear();
		    origData.clear();
		    origData.putAll(data);

		    postLoad();
		}
    }

    public synchronized void revert() {
		isModified = false;
		modTable.clear();

		data.clear();
		data.putAll(origData);
    }

    public synchronized void delete() throws DaoException {
		if (!preDelete()) {
		    return;
		}

		if (hasResource()) {
		    getResource().delete(getId());
		}

		isDeleted = true;

		postDelete();
    }

    public synchronized void save() throws DaoException {
    	save(false, null);
    }
    
    private void resetStateAfterSave() throws DaoException {
		origData.putAll(data);
		isModified = false;
		modTable.clear();

		postSave();
    }
    
    protected Map<String, Object> createInsertMap() {		
		return new HashMap<String, Object>(data);
    }
    
    protected Map<String, Object> createUpdateMap() {
    	HashMap<String, Object> updateMap = new HashMap<String, Object>();

		// only push modified values to resource adapter
		for (String key : modTable) {
			updateMap.put(key, data.get(key));
		}
		
		return updateMap;
    }
    
    private void save(boolean async, AsyncCallback callback) throws DaoException {
    	boolean wasNullId = (this.id == null);
    	
		if (!preSave() || !isModified) {
		    return;
		}

		if (hasResource()) {
			// especially nosql resource adapters have some strange behaviors
            // which can be exposed via capabilities

		    if (checkResourceForCapability(com.sis.dao.Resource.CAPABILITY_UPDATE) &&
	    		(!wasNullId || checkResourceForCapability(com.sis.dao.Resource.CAPABILITY_INSERT_BY_UPDATE))) {
		    	if (!checkResourceForCapability(com.sis.dao.Resource.CAPABILITY_NEED_FULL_UPDATE)) {
		    		Map<String, Object> updateMap = createUpdateMap();

		    		if (async) {
		    			asyncCommands.add(new DataObjectCommandPacket(this, updateMap, DataObjectCommand.UPDATE, callback));
		    		} else {
			    		getResource().update(id, updateMap);	
		    		}
		    	} else {
		    		Map<String, Object> insertMap = createInsertMap();
		    		
		    		if (async) {
			        	asyncCommands.add(new DataObjectCommandPacket(this, insertMap, DataObjectCommand.UPDATE, callback));	
		    		} else {
			    		getResource().update(id, insertMap);	
		    		}
		    	}
		    	
		    	isNew  = false;
		    } else if (checkResourceForCapability(com.sis.dao.Resource.CAPABILITY_INSERT)) {
	    		Map<String, Object> insertMap = createInsertMap();
	    		
		    	// in case preSave set Id, let's transmit id in data packet.
		    	if (wasNullId && getId() != null) {
		    		insertMap.put(idFieldName, getId());

		    		if (async) {
		    			asyncCommands.add(new DataObjectCommandPacket(this, insertMap, DataObjectCommand.INSERT_WITHOUT_SET_ID, callback));
		    		} else {
			    		getResource().insert(data);	
		    		}
		    	} else {
		    		if (async) {
		    			asyncCommands.add(new DataObjectCommandPacket(this, insertMap, DataObjectCommand.INSERT, callback));
		    		} else {
			    		setId(getResource().insert(insertMap));
		    		}
		    	}
		    	
		    	isNew = true;
		    }
		}
		
		if (!async) {
			resetStateAfterSave();
		}
    }

    public <T> T getData(String key, Class<T> cast) {
    	return getData(key, cast, null);
    }
    
    @SuppressWarnings("unchecked")
	public <T> T getData(String key, Class<T> cast, T defaultValue) {
		try {
			Object o = data.get(key);
			
			if (o == null) {
				return defaultValue;
			}
			
			// workaround for broken string casts
			if (String.class.isAssignableFrom(cast)) {
				return (T) o.toString();
			}
			
			return cast.cast(o);
		} catch(Exception e) {
			logger.warn("error while casting data.", e);
			
			return null;
		}
    }
    
    @Deprecated
    public java.lang.Object getData(String key) {
		return hasData(key) ? data.get(key) : null;
    }

    public void setData(Map <String, java.lang.Object> data) {
		isModified = true;
		this.data.clear();
		this.data.putAll(data);

		for (String key : data.keySet()) {
		    this.modTable.add(key);
		}
    }

    public void setData(String key, java.lang.Object value) {
    	if (hasData(key) && getData(key) != null && getData(key).toString().equals(value.toString())) {
    		return;
    	}

		if (key.compareTo(idFieldName) == 0) {
		    id  = value.toString();
		} else {
    		data.put(key, value);
    		this.modTable.add(key);
		}
		isModified = true;
    }

    public boolean hasData(String key) {
    	return data.containsKey(key);
    }

    public void unsetData(String key) {
    	data.remove(key);
    }

    public boolean isModified() {
		return isModified;
	}
    
    protected boolean isModified(String field) {
    	return modTable.contains(field);
    }

	public boolean isDeleted() {
		return isDeleted;
	}

    public boolean isNew() {
		return isNew;
	}

	public void setNew(boolean isNew) {
		this.isNew = isNew;
	}

	protected void addTag(String field, String tag) {
		if (!tagAdds.containsKey(field)) {
			tagAdds.put(field, Collections.synchronizedSet(new HashSet<String> ()));
		}
		
		tagAdds.get(field).add(tag);
		// FIXME: better remove only the value from field, not the entire field.
		tagRemoves.remove(field);
		
		isModified = true;
	}
	
	protected void removeTag(String field, String tag) {
		if (!tagRemoves.containsKey(field)) {
			tagRemoves.put(field, Collections.synchronizedSet(new HashSet<String> ()));
		}
		
		tagRemoves.get(field).add(tag);
		// FIXME: better remove only the value from field, not the entire field.
		tagAdds.remove(field);
		
		isModified = true;
	}
	
	protected void incrementField(String field, int delta) {
		fieldIncrements.put(field, delta);
		
		isModified = true;
	}
	
	public HashMap<String, java.lang.Object> toArray() {
    	HashMap<String, java.lang.Object> array = new HashMap<String, java.lang.Object>();

    	array.put(idFieldName, id);
    	array.putAll(data);

    	return array;
    }

    public String toJson() {
    	return JSON.defaultJSON().forValue(toArray());
    }
}
