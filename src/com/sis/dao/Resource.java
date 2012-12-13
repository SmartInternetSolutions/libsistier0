package com.sis.dao;

import java.util.Map;

public interface Resource {
    final int CAPABILITY_INSERT             = 1;
    final int CAPABILITY_UPDATE             = 2;
    final int CAPABILITY_DELETE             = 4;
    
    final int CAPABILITY_TRANSACTION        = 64;
    
    final int CAPABILITY_PRIMARYKEY         = 128;
    
    final int CAPABILITY_NEED_FULL_UPDATE   = 1024;
    final int CAPABILITY_INSERT_BY_UPDATE   = 2048;
    
    public int getCapabilities();
    
    public void load(DataObject object, String value, String field) throws DaoException;
	public void loadCollection(Collection collection, java.lang.Object[] args) throws DaoException;
    
    public String insert(Map<String, java.lang.Object> fields) throws DaoException;
    public boolean update(String id, Map<String, java.lang.Object> updateMap) throws DaoException;
    public boolean delete(String id) throws DaoException;
    
    public void beginTransaction() throws DaoException;
    public void commit() throws DaoException;
    public void rollback() throws DaoException;
}
