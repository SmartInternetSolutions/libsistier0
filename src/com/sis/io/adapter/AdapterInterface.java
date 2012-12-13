package com.sis.io.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * 
 * An adapter represents abstraction for a scheme. See implementations.
 * 
 * @author CR
 *
 */
public interface AdapterInterface {
	/**
	 * does the file exist
	 * 
	 * @param uri
	 * @return
	 */
	public boolean exists(URI uri) throws IOException;
	
	/**
	 * copies file from src to dst
	 * 
	 * @param src
	 * @param dst
	 * @throws IOException
	 */
	public void copyFile(URI src, URI dst) throws IOException;
	
	/**
	 * moves file from src to dst
	 * 
	 * @param src
	 * @param dst
	 * @throws IOException
	 */
	public void moveFile(URI src, URI dst) throws IOException;
	
	/**
	 * Creates file and returns its stream.
	 * 
	 * @param uri
	 * @return
	 * @throws IOException
	 */
	public OutputStream createFile(URI uri) throws IOException;
	
	/**
	 * Opens file for write and returns stream which appends to the file.
	 * If there's no file, it will be created.
	 * 
	 * @param uri
	 * @return
	 * @throws IOException
	 */
	public OutputStream appendFile(URI uri) throws IOException;
	
	/**
	 * Opens given URI for read 
	 * 
	 * @param uri
	 * @return
	 * @throws IOException
	 */
	public InputStream openFile(URI uri) throws IOException;
	
	/**
	 * deletes given URI
	 * 
	 * @param uri
	 * @throws IOException
	 */
	public void deleteFile(URI uri) throws IOException;
	
}
