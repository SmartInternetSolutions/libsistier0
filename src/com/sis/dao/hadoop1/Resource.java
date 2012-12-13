package com.sis.dao.hadoop1;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.svenson.JSON;

import com.sis.dao.Collection;
import com.sis.dao.DaoException;
import com.sis.dao.DataObject;
import com.sis.io.SmartCDN;

public class Resource implements com.sis.dao.Resource {
	private static final transient Logger logger = Logger.getLogger("Hadoop1");
	
	protected String cluster = "";
	protected String group = "";
	
	public String getCluster() {
		return cluster;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}
	
	public URI getFileUri() {
		try {
			return new URI(SmartCDN.getCategoryRootURI("tracking") + "sds/hadoop1/" + group + "/" + cluster + ".log");
		} catch (URISyntaxException e) {
			logger.error("could not create uri!", e);
			
			return null;
		}
	}

	@Override
	public int getCapabilities() {
		return CAPABILITY_INSERT | CAPABILITY_PRIMARYKEY;
	}

	@Override
	public void load(DataObject object, String value, String field) throws DaoException {
		// NOT SUPPORTED
	}

	@Override
	public void loadCollection(Collection collection, Object[] args) throws DaoException {
	}

	@Override
	public String insert(Map<String, Object> fields) throws DaoException {
		try {
			OutputStream logfile = SmartCDN.appendFile(getFileUri());

			logfile.write(JSON.defaultJSON().forValue(fields).getBytes("UTF-8"));
			logfile.write('\n');
			logfile.close();
		} catch (IOException e) {
			logger.error("could not write to database!", e);
		}
		
		return null;
	}

	@Override
	public boolean update(String id, Map<String, Object> updateMap) throws DaoException {
		// NOT SUPPORTED
		return false;
	}

	@Override
	public boolean delete(String id) throws DaoException {
		// NOT SUPPORTED
		return false;
	}

	@Override
	public void beginTransaction() throws DaoException {
		// NOT SUPPORTED
	}

	@Override
	public void commit() throws DaoException {
		// NOT SUPPORTED
	}

	@Override
	public void rollback() throws DaoException {
		// NOT SUPPORTED
	}

}
