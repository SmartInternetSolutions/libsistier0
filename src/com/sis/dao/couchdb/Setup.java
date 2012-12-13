package com.sis.sds.dao.couchdb;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;

import org.svenson.JSON;

import com.sis.sds.core.Config;

public class Setup {
	private static String sendRequest(String uri, String payload) throws IOException {
		URL url = new URL(
			"http://" + Config.getSingleton().getValue("couchdb/master/address") + "/" + Config.getSingleton().getValue("couchdb/master/database") + "/" + uri
		);
		
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		
		connection.setRequestMethod("POST");
		connection.setRequestProperty("Content-Type", "application/json");
		connection.setRequestProperty("Content-Length", payload.length());

		connection.setUseCaches(false);
		connection.setDoInput(true);
		connection.setDoOutput(true);

		DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
		wr.writeBytes(payload);
		wr.flush();
		wr.close();
		
		InputStream is = connection.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);

		StringBuilder sb = new StringBuilder();
		
		String line = null;
		while ((line = br.readLine()) != null) {
			sb.append(line);
		}
		
		br.close();
		isr.close();
		is.close();
		
		return sb.toString();
	}
	
	public static void setupUpdateHandlers() throws IOException {
		HashMap <String, HashSet<?>> map = new HashMap <String, HashSet<?>>();
		HashSet<HashMap<String, java.lang.Object>> list = new HashSet<HashMap<String, java.lang.Object>>();
		HashMap<String, java.lang.Object> map2 = new HashMap<String, java.lang.Object>();
		HashMap<String, String> updates = new HashMap<String, String>();
		map2.put("_id", "_design/app");
		map2.put("updates", updates);
		
		// in-place
		updates.put("in-place", 
			"function (doc, req) {" +
				"var newFields = JSON.parse(req.body); " +
				"var counter = 0; " +
				"for (var key in newFields) { " +
					"doc[key] = newFields[key]; " +
					"counter++;" + 
				"} " +
				"return [doc, 'updated ' + counter + ' field(s)'];" + 
			"}"
		);
		
		// adjust field value
		updates.put("adjust", 
				"function (doc, req) {" +
				"var newFields = JSON.parse(req.body); " +
				"var counter = 0; " +
				"for (var key in newFields) { " +
					"if (typeof(doc[key]) === 'undefined') { " +
						"doc[key] = 0; " +
					"} " +
					"doc[key] += parseInt(newFields[key]); " +
					"counter++;" + 
				"} " +
				"return [doc, 'updated ' + counter + ' field(s)'];" + 
			"}"
		);

		// add tag
		updates.put("manage-tags", 
			"function (doc, req) {" +
				"var body = JSON.parse(req.body);" +
				"var addTags = body.addTags; " +
				"var removeTags = body.removeTags; " +
				"var counter = 0; " +
				
				// TODO: implement loop for removeTags
				// TODO: optimize slightly
				
				"for (var key in addTags) { " +
					"if (typeof(doc[key]) === 'undefined') { " +
						"doc[key] = []; " +
					"} else if (!doc[key] instanceof Array) { " +
						"doc[key] = [doc[key]]; " +
					"} " +
					
					"if (!addTags[key] instanceof Array) {" +
						"addTags[key] = [addTags[key]]; " +
					"} " +
					
					"for (var key2 in addTags[key]) { " +
						"if (doc[key].indexOf(addTags[key][key2]) === -1) { " +
							"doc[key].push(addTags[key][key2]); " + 
						"} " +
					"} " +
					
					"counter++;" + 
				"} " +
				
				"return [doc, 'updated ' + counter + ' field(s)'];" + 
			"}"
		);

		list.add(map2);
		map.put("docs", list);
		
		System.out.println(sendRequest("_bulk_docs/", JSON.defaultJSON().forValue(map)));
	}
}
