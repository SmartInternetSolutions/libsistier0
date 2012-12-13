package com.sis.io.adapter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;

/**
 * Filesystem implementation.
 * 
 * @author CR
 *
 */
public class Local implements AdapterInterface {

	@Override
	public boolean exists(URI uri) throws IOException {
		return new File(uri).exists();
	}

	@Override
	public void copyFile(URI src, URI dst) throws IOException {
		Files.copy(new File(src).toPath(), new File(dst).toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
	}

	@Override
	public void moveFile(URI src, URI dst) throws IOException {
		Files.move(new File(src).toPath(), new File(dst).toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
	}

	@Override
	public OutputStream createFile(URI uri) throws IOException {
		File file = new File(uri);
		file.createNewFile();
		
		return new FileOutputStream(file);
	}

	@Override
	public OutputStream appendFile(URI uri) throws IOException {
		File file = new File(uri);
		file.createNewFile();
		
		return new FileOutputStream(file);
	}

	@Override
	public InputStream openFile(URI uri) throws IOException {
		File file = new File(uri);
		
		return new FileInputStream(file);
	}

	@Override
	public void deleteFile(URI uri) throws IOException {
		new File(uri).delete();
	}

}
