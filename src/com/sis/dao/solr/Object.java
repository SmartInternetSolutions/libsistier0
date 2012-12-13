package com.sis.dao.solr;

import com.sis.dao.DaoException;

public class Object extends com.sis.dao.DataObject {
	public Object() {
		init("id", new Resource());
	}
	
	protected boolean preSave() throws DaoException  {
		if (getId() == null) {
			throw new com.sis.dao.DaoException("id must be set before save on SolrDocument");
		}
		
		return true;
	}
}
