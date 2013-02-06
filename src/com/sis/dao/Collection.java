package com.sis.dao;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.NoSuchElementException;

import com.sis.util.Pair;

public class Collection implements Iterable<DataObject> {
	protected Resource resource 					= null;
	protected LinkedHashMap<String, DataObject> items 	= new LinkedHashMap<String, DataObject>();
    protected String tableName						= "";
    protected String idFieldName 					= "id";

    protected boolean loaded	= false;

	@Override
	public Iterator<DataObject> iterator() {
		return items.values().iterator();
	}

	public void load() throws DaoException {
		if (!loaded) {
			loadCollection();
			loaded = true;
		}
	}

	protected void loadCollection() throws DaoException {
	}

	public void delete() throws DaoException {
		synchronized (items) {
			load();

			resource.beginTransaction();

			try {
				for (DataObject object : items.values()) {
					object.delete();
				}

				clear();
			} catch(DaoException e) {
				resource.rollback();
				throw e;
			}

			resource.commit();
		}
	}
	
	public void save() throws DaoException {
		synchronized (items) {
			resource.beginTransaction();
			
			try {
				for (DataObject object : items.values()) {
					object.save();
				}
				
				clear();
			} catch(DaoException e) {
				resource.rollback();
				throw e;
			}
			
			resource.commit();
		}
	}

	public void addItem(DataObject item) {
		items.put(item.getId(), item);
	}

	public DataObject getFirstElement() throws DaoException {
		load();
		try {
			return iterator().next();
		} catch(NoSuchElementException e) {
			return null;
		}
	}

	public String getTableName() {
		return tableName;
	}

	public String getIdFieldName() {
		return idFieldName;
	}

	protected DataObject _newItem() {
		return null;
	}

	final public DataObject newItem() {
		DataObject dobj = _newItem();
		
		if (dobj == null) {
			throw new RuntimeException("_newItem() returns null! Please inherit _newItem() correctly!");
		}
		
		return dobj;
	}

	public void removeItemById(String id) {
		if (items.containsKey(id)) {
			items.remove(id);
		}
	}

	public long size() {
		return items.size();
	}

	/**
	 * resets filters
	 */
	public void reset() {
		fieldFilters.clear();
	}

	/**
	 * clears out collection (implies also reset())
	 */
	public void clear() {
		reset();
		items.clear();
	}

    protected boolean hasResource() {
    	return resource != null;
    }

    protected boolean checkResourceForCapability(int cap) {
    	return hasResource() && (resource.getCapabilities() & cap) != 0;
    }

    /// SORT
    
    final public static int SORT_UNDEFINED				= 0;
    final public static int SORT_ASCENDING				= 1;
    final public static int SORT_DESCENDING				= 2;

    protected HashMap<String, Integer> fieldSortings = new HashMap<String, Integer>();
    
	public void addFieldToSort(String string, int direction) {
		fieldSortings.put(string, direction);
	}
    
    /// FILTER METHODS (backed by resource then)

    final public static int FILTER_CUSTOM 				= 0;
    final public static int FILTER_EQUALS 				= 1;
    final public static int FILTER_LIKE 				= 2;
    final public static int FILTER_LESS					= 3;
    final public static int FILTER_LESS_OR_EQUALS		= 4;
    final public static int FILTER_GREATER				= 5;
    final public static int FILTER_GREATER_OR_EQUALS	= 6;
    final public static int FILTER_NOT_EQUALS 			= 7;
    final public static int FILTER_FIND_IN_SET			= 8;
    final public static int FILTER_NOT_IN_SET			= 9;

	protected final HashMap<String, Pair<java.lang.Object[], Integer>> fieldFilters = new HashMap<String, Pair<java.lang.Object[], Integer>>();

	public void addFieldToFilter(String field, java.lang.Object[] value, int mode) {
		fieldFilters.put(field, new Pair<java.lang.Object[], Integer>(value, mode));
	}

	private static final Object[] nullObjectArray = new java.lang.Object[] {null};
	
	public void addFieldToFilter(String field, java.lang.Object[] value) {
		addFieldToFilter(field, value != null ? value : nullObjectArray, Collection.FILTER_EQUALS);
	}

	public void addFieldToFilter(String field, java.lang.Object value, int mode) {
		addFieldToFilter(field, new java.lang.Object[] {value}, mode);
	}

	public void addFieldToFilter(String field, java.lang.Object value) {
		addFieldToFilter(field, new java.lang.Object[] {value});
	}

	/// -----------------------------------------------------------------------
	
	protected long limitCount = 0, limitOffset = 0;

	public void limit(long count) {
		limit(count, 0);
	}

	public void limit(long count, long offset) {
		limitCount = count;
		limitOffset = offset;
	}

	public long getCount() {
		return 0; // IMPLEMENT ME
	}
}
