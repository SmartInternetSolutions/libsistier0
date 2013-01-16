package com.sis.dao;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.svenson.JSON;

import com.sis.system.Base;
import com.sis.util.Pair;

public class DataObject {
    protected String id				= null;

    protected String idFieldName 	= "id";

	protected transient Resource resource 	= null;

    protected String tableName						= "";
    protected Map<String, java.lang.Object> data	= null;

    protected Map<String, java.lang.Object> origData	= null;
    private boolean isModified							= false;

	protected Set<String> modTable						= null;

    private boolean isDeleted = false, isNew = false;

    public DataObject() {
		data 		= new HashMap<String, java.lang.Object>();
		origData	= new HashMap<String, java.lang.Object>();
		modTable	= new HashSet<String>();
    }

    private enum AsyncAction {
    	DELETE,
    	SAVE
    }
    
    private static final Logger logger = Logger.getLogger(DataObject.class);
    
    private static class BackloggedAsyncAction {
    	private AsyncAction asyncAction;
    	private DataObject dataObject;
		
		private int retryCounter = 0;
    	
    	public BackloggedAsyncAction(DataObject object, AsyncAction action) {
    		setAsyncAction(action);
    		setDataObject(object);
		}

		public AsyncAction getAsyncAction() {
			return asyncAction;
		}
    	
		public void setAsyncAction(AsyncAction asyncAction) {
			this.asyncAction = asyncAction;
		}
		
		public DataObject getDataObject() {
			return dataObject;
		}
		
		public void setDataObject(DataObject dataObject) {
			this.dataObject = dataObject;
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
    	
    }
    
    private static final LinkedBlockingQueue<BackloggedAsyncAction> asyncActions = new LinkedBlockingQueue<>();
    private static final Thread asyncHandlerThread;
    
    static {
    	asyncHandlerThread = new Thread(new Runnable() {
			@Override
			public void run() {
				asyncHandlerThread.setName("DataObject Asynchronous Queue Worker");
				
				// CR: let's group by asyncAction and put through a single transaction?
				while (!Base.isShuttingDown()) {
					BackloggedAsyncAction baa = null;
					
					try {
						while ((baa = asyncActions.poll(3, TimeUnit.SECONDS)) != null) {
							if (baa.getRetryCounter() > 10) {
								logger.debug("stopped retrying of backlogged async action.");
							}
							
							try {
								switch (baa.getAsyncAction()) {
								case DELETE:
									baa.getDataObject().delete();
									break;
									
								case SAVE:
									baa.getDataObject().save();
									break;
								}
							} catch(DaoException e) {
								logger.warn("async dao exception, re-putting to queue", e);
								baa.increaseRetryCounter();
								asyncActions.add(baa);
							} catch(Exception e) {
								logger.error("async action failed!", e);
							}
						}
					} catch (InterruptedException e) {
					}
					
					logger.debug("finished async action queue");
				}
			}
		});
    	
    	asyncHandlerThread.start();
    }
    
    /**
     * waits until async queue is at normal size again
     */
    private static void waitForSpace() {
    	Runtime rt = Runtime.getRuntime();

    	if (asyncActions.size() > 10000) {
    		logger.warn("async queue is getting very large, waiting for async actions to finish. freeMem = " + rt.freeMemory() + ", totalMem = " + rt.totalMemory() + ", size = " + asyncActions.size());
    	
	    	while (asyncActions.size() > 0) {
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
    public void saveAsync() {
    	BackloggedAsyncAction baa = new BackloggedAsyncAction(this, AsyncAction.SAVE);
    	
    	asyncActions.add(baa);
    	
    	waitForSpace();
    }

    /**
     * queues delete command in a fifo backlog. no blocking at all. (errors will be ignored!)
     */
    public void deleteAsync() {
    	BackloggedAsyncAction baa = new BackloggedAsyncAction(this, AsyncAction.DELETE);
    	
    	asyncActions.add(baa);
    	
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

    public void load(String value, String field) throws DaoException {
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

    public void revert() {
		isModified = false;
		modTable.clear();

		data.clear();
		data.putAll(origData);
    }

    public void delete() throws DaoException {
		if (!preDelete()) {
		    return;
		}

		if (hasResource()) {
		    getResource().delete(getId());
		}

		isDeleted = true;

		postDelete();
    }

    public void save() throws DaoException {
    	boolean wasNullId = (id == null);
    	
		if (!preSave() || !isModified) {
		    return;
		}

		if (hasResource()) {
			// especially nosql resource adapters have some strange behaviors
            // which can be exposed via capabilities

		    if (checkResourceForCapability(com.sis.dao.Resource.CAPABILITY_UPDATE) &&
	    		(!wasNullId || checkResourceForCapability(com.sis.dao.Resource.CAPABILITY_INSERT_BY_UPDATE))) {
		    	if (!checkResourceForCapability(com.sis.dao.Resource.CAPABILITY_NEED_FULL_UPDATE)) {
		    		HashMap<String, java.lang.Object> updateMap = new HashMap<String, java.lang.Object>();

		    		// only push modified values to resource adapter
		    		synchronized (modTable) {
			    		for (String key : modTable) {
			    			updateMap.put(key, data.get(key));
						}
					}

		    		getResource().update(id, updateMap);
		    	} else {
		    		getResource().update(id, data);
		    	}
		    	
		    	isNew  = false;
		    } else if (checkResourceForCapability(com.sis.dao.Resource.CAPABILITY_INSERT)) {
		    	// in case preSave set Id, let's transmit id in data packet.
		    	if (wasNullId && getId() != null) {
		    		data.put(idFieldName, getId());
		    		
		    		getResource().insert(data);
		    	} else {
		    		setId(getResource().insert(data));
		    	}
		    	
		    	isNew = true;
		    }
		}

		origData.putAll(data);
		isModified = false;
		modTable.clear();

		postSave();
    }

    public <T> T getData(String key, Class<T> cast) {
    	return getData(key, cast, null);
    }
    
    public <T> T getData(String key, Class<T> cast, T defaultValue) {
		try {
			if (!data.containsKey(key)) {
				return defaultValue;
			}
			
			return cast.cast(data.get(key));
		} catch(Exception e) {
			try {
				return cast.newInstance();
			} catch (InstantiationException | IllegalAccessException e2) {
				return null;
			}
		}
    }
    
    public java.lang.Object getData(String key) {
		return hasData(key) ? data.get(key) : null;
    }

    public void setData(Map <String, java.lang.Object> data) {
		isModified = true;
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

	public boolean isDeleted() {
		return isDeleted;
	}

    public boolean isNew() {
		return isNew;
	}

	public void setNew(boolean isNew) {
		this.isNew = isNew;
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
