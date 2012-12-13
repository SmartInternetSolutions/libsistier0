package com.sis.io.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

/**
 * @todo re-implement by using commons-httpclient
 * @author CR
 *
 */
public class SimpleHttp implements AdapterInterface {
	private static URL fromURItoURL(URI uri) throws IOException  {
		try {
			URL url = uri.toURL();

			return url;
		} catch (MalformedURLException e) {
			throw new IOException(e);
		}
	}
	
	private static HttpURLConnection openConnection(URL url) throws IOException {
		return (HttpURLConnection) url.openConnection();
	}

	private static HttpURLConnection openConnection(URI uri) throws IOException {
		HttpURLConnection conn = openConnection(fromURItoURL(uri));

		conn.setReadTimeout(2500);
		conn.setConnectTimeout(2500);
		conn.connect();
		
		return conn;
	}
	
	@Override
	public boolean exists(URI uri) throws IOException {
		HttpURLConnection conn = openConnection(uri);
		boolean exists = conn.getResponseCode() == HttpURLConnection.HTTP_OK;
		
		conn.disconnect();
		
		return exists;
	}

	@Override
	public void copyFile(URI src, URI dst) throws IOException {
		throw new IOException("copyFile: not implemented");
	}

	@Override
	public void moveFile(URI src, URI dst) throws IOException {
		throw new IOException("moveFile: not implemented");
	}

	@Override
	public OutputStream createFile(URI uri) throws IOException {
		throw new IOException("createFile: not implemented");
	}

	@Override
	public OutputStream appendFile(URI uri) throws IOException {
		throw new IOException("appendFile: not implemented");
	}

	@Override
	public InputStream openFile(URI uri) throws IOException {
		return openConnection(uri).getInputStream();
	}

	@Override
	public void deleteFile(URI uri) throws IOException {
		throw new IOException("deleteFile: not implemented");
	}

}
