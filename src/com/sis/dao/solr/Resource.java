package com.sis.dao.solr;

import java.net.MalformedURLException;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import com.sis.dao.Collection;
import com.sis.dao.DaoException;
import com.sis.dao.DataObject;
import com.sis.system.XmlConfig;


public class Resource implements com.sis.dao.Resource {
	private static CommonsHttpSolrServer solrCore = null;

	protected static void connectSolr() {
		if (solrCore == null) {
			try {
				solrCore = new CommonsHttpSolrServer(XmlConfig.getSingleton().getValue("solr/url"));
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public int getCapabilities() {
		return 	CAPABILITY_UPDATE |
				CAPABILITY_DELETE |
				CAPABILITY_PRIMARYKEY |
				CAPABILITY_INSERT_BY_UPDATE |
				CAPABILITY_NEED_FULL_UPDATE ;
	}

	@Override
	public void load(DataObject object, String value, String field) throws DaoException {
		connectSolr();

		SolrQuery query = new SolrQuery();
		query.setQuery(field + ":\"" + value + "\"");
		query.setRows(1);

		QueryResponse res = null;
		try {
			res = solrCore.query(query);
		} catch (SolrServerException e) {
			throw new com.sis.dao.DaoException("couldn't connect resource (" + e.getMessage() + ")");
		}

		SolrDocumentList docs = res.getResults();

		if (docs.size() > 0) {
			SolrDocument doc = docs.get(0);

			for (String key : doc.keySet()) {
				object.setData(key, doc.getFieldValue(key));
			}
		} else {
			// TODO: through exception
		}
	}

	@Override
	public String insert(Map<String, java.lang.Object> fields) {
		connectSolr();

		return null;
	}

	@Override
	public boolean update(String id, Map<String, java.lang.Object> fields) {
		connectSolr();

		return false;
	}

	@Override
	public boolean delete(String id) {
		connectSolr();

		return false;
	}

	@Override
	public void beginTransaction() {
		// TODO Auto-generated method stub

	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub

	}

	@Override
	public void rollback() {
		// TODO Auto-generated method stub

	}

	@Override
	public void loadCollection(Collection collection, java.lang.Object[] args) throws DaoException {
		// TODO Auto-generated method stub

	}
}
