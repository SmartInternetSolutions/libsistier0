package com.sis.io.adapter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Date;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.sis.system.ConfigurableVariable;

/**
 * Amazon AWS S3 implementation.
 * 
 * You need to specify the credentials in the config.xml
 *  <s3>
 *   <username>access key id</username>
 *   <password>access secret</password>
 *  </s3>
 * 
 * You can access then via
 *  s3://<bucketname>/<filename>
 * 
 * @todo append file
 * @note please do not upload really big files, streaming is not supported
 * @author CR
 *
 */
public class S3 implements AdapterInterface {
	private static final Logger logger = Logger.getLogger(S3.class);
	
	private static class BucketNameKeyPair {
		private String bucketName;
		private String key;

		public String getBucketName() {
			return bucketName;
		}

		public void setBucketName(String bucketName) {
			this.bucketName = bucketName;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}
		
		public static BucketNameKeyPair createByURI(URI uri) {
			BucketNameKeyPair pair = new BucketNameKeyPair();
			
			String key = uri.getPath();
			while (!key.isEmpty() && key.charAt(0) == '/') {
				key = key.substring(1);
			}
			
			pair.setBucketName(uri.getHost());
			pair.setKey(key);
			
			return pair;
		}
	}
	
	private static class S3OutputStream extends OutputStream {
		private final URI targetUri;
		private final ByteArrayOutputStream stream;
		
		public S3OutputStream(URI uri) throws IOException {
			targetUri = uri;
			stream = new ByteArrayOutputStream(1024);
		}
		
		@Override
		public synchronized void write(int b) throws IOException {
			stream.write(b);
		}
		
		@Override
		public void close() throws IOException {
			BucketNameKeyPair pair = BucketNameKeyPair.createByURI(targetUri);
			
			stream.flush();
			
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(stream.size());

			getAmazonS3(targetUri).putObject(pair.getBucketName(), pair.getKey(), new ByteArrayInputStream(stream.toByteArray()), metadata);
			
			super.close();
		}
		
		@Override
		public void flush() throws IOException {
			super.flush();
		}
	}
	
	private final static ConfigurableVariable<String> s3username	= new ConfigurableVariable<String>("s3/username", "");
	private final static ConfigurableVariable<String> s3password	= new ConfigurableVariable<String>("s3/password", "");
	
	private final static AmazonS3 AMAZON_S3 = new AmazonS3Client(new BasicAWSCredentials(s3username.getValue(), s3password.getValue()));
	
	protected static AmazonS3 getAmazonS3(URI uri) {
		return AMAZON_S3;
	}
	
	private static S3Object getS3Object(URI uri) {
		BucketNameKeyPair pair = BucketNameKeyPair.createByURI(uri);
		
		try {
			return getAmazonS3(uri).getObject(new GetObjectRequest(pair.getBucketName(), pair.getKey()));
		} catch (AmazonServiceException e) {
			logger.debug(e);
			
			return null;
		}
	}
	
	
	@Override
	public boolean exists(URI uri) throws IOException {
		return getS3Object(uri) != null;
	}

	@Override
	public void copyFile(URI src, URI dst) throws IOException {
		BucketNameKeyPair srcPair = BucketNameKeyPair.createByURI(src);
		BucketNameKeyPair dstPair = BucketNameKeyPair.createByURI(dst);
		
		getAmazonS3(src).copyObject(srcPair.getBucketName(), srcPair.getKey(), dstPair.getBucketName(), dstPair.getKey());
	}

	@Override
	public void moveFile(URI src, URI dst) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public OutputStream createFile(URI uri) throws IOException {
		return new S3OutputStream(uri);
	}

	@Override
	public OutputStream appendFile(URI uri) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public InputStream openFile(URI uri) throws IOException {
		S3Object obj = getS3Object(uri);
		
		if (obj == null) {
			throw new FileNotFoundException("object not found");
		}
		
		return obj.getObjectContent();
	}

	@Override
	public void deleteFile(URI uri) throws IOException {
		BucketNameKeyPair pair = BucketNameKeyPair.createByURI(uri);
		
		try {
			getAmazonS3(uri).deleteObject(new DeleteObjectRequest(pair.getBucketName(), pair.getKey()));
		} catch (AmazonServiceException e) {
			throw new IOException("unable to delete file from bucket!", e);
		}
	}
}
