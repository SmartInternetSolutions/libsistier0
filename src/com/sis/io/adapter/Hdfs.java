package com.sis.io.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * Hadoop DFS implementation.
 * 
 * @author CR
 *
 */
public class Hdfs implements AdapterInterface {
	private static final HashMap<String, FileSystem> hadoopConnections = new HashMap<String, FileSystem>();
	private static Configuration hadoopConf = new Configuration();
	
	private static FileSystem getHadoopFilesystemByUri(URI uri) {
		String url = "hdfs://" + uri.getHost() + ":" + uri.getPort() + "/";

		if (!hadoopConnections.containsKey(uri)) {
			try {
				hadoopConnections.put(url, FileSystem.get(new URI(url), hadoopConf));
			} catch (URISyntaxException e) {
				// should never happen
				return null;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}

		return hadoopConnections.get(url);
	}
	
	@Override
	public boolean exists(URI uri) throws IOException {
		return getHadoopFilesystemByUri(uri).exists(new Path(uri));
	}

	@Override
	public void copyFile(URI src, URI dst) throws IOException {
		FileSystem srcfs = getHadoopFilesystemByUri(src);
		FileSystem dstfs = getHadoopFilesystemByUri(dst);
		
		FileUtil.copy(srcfs, new Path(src.getPath()), dstfs, new Path(dst.getPath()), true, hadoopConf);
	}

	@Override
	public void moveFile(URI src, URI dst) throws IOException {
		getHadoopFilesystemByUri(src).rename(new Path(src.getPath()), new Path(dst.getPath()));
	}

	@Override
	public OutputStream createFile(URI uri) throws IOException {
		return getHadoopFilesystemByUri(uri).create(new Path(uri.getPath()), true);
	}

	@Override
	public OutputStream appendFile(URI uri) throws IOException {
		FileSystem fs = getHadoopFilesystemByUri(uri);
		Path path = new Path(uri.getPath());
		
		return fs.exists(path) ? fs.append(path) : fs.create(path);
	}

	@Override
	public InputStream openFile(URI uri) throws IOException {
		return getHadoopFilesystemByUri(uri).open(new Path(uri.getPath()));
	}

	@Override
	public void deleteFile(URI uri) throws IOException {
		getHadoopFilesystemByUri(uri).delete(new Path(uri.getPath()), true);
	}
}
