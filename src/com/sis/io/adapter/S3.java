package com.sis.io.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * Amazon AWS S3 implementation.
 * 
 * @todo unusable
 * @author CR
 *
 */
public class S3 implements AdapterInterface {

	@Override
	public boolean exists(URI uri) throws IOException {
		return false;
	}

	@Override
	public void copyFile(URI src, URI dst) throws IOException {
	}

	@Override
	public void moveFile(URI src, URI dst) throws IOException {
	}

	@Override
	public OutputStream createFile(URI uri) throws IOException {
		return null;
	}

	@Override
	public OutputStream appendFile(URI uri) throws IOException {
		return null;
	}

	@Override
	public InputStream openFile(URI uri) throws IOException {
		return null;
	}

	@Override
	public void deleteFile(URI uri) throws IOException {
	}
}
