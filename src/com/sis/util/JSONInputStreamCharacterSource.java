package com.sis.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;
import org.svenson.tokenize.JSONCharacterSource;

/**
 * to be used with {@link org.svenson.JSONParser} to avoid full buffering of stream
 * 
 * JSONParser.defaultJSONParser().parse(new JSONInputStreamCharacterSource(inputStream));
 * 
 * @author CR
 *
 */
public class JSONInputStreamCharacterSource implements JSONCharacterSource {
	private static final Logger logger = Logger.getLogger(JSONInputStreamCharacterSource.class);
	private final InputStream inputStream;
	private final InputStreamReader inputStreamReader;
	
	private int index = 0;
	
	private int maxLength = 0;
	
	private boolean closed = false;
	
	public JSONInputStreamCharacterSource(InputStream is, String encoding) throws UnsupportedEncodingException {
		inputStream = is;
		inputStreamReader = new InputStreamReader(is, encoding);
	}
	
	public JSONInputStreamCharacterSource(InputStream is, int maxLen, String encoding) throws UnsupportedEncodingException {
		this(is, encoding);
		setMaxLength(maxLen);
	}

	public void setMaxLength(int maxLen) {
		maxLength = maxLen;
	}
	
	@Override
	protected void finalize() throws Throwable {
		destroy();
		
		super.finalize();
	}
	
	@Override
	public void destroy() {
		try {
			if (!closed) {
				inputStreamReader.close();
				inputStream.close();
				
				closed = true;
			}
		} catch (IOException e) {
			logger.error("could not close stream!", e);
		}
	}

	@Override
	public int getIndex() {
		return index;
	}

	@Override
	public int nextChar() {
		if (maxLength > 0 && getIndex() > maxLength) {
			return 0;
		}
		
		try {
			int c = inputStreamReader.read();
			
			index++;
			
			return c;
		} catch (IOException e) {
			logger.error("could not read stream!", e);
		}
		
		return 0;
	}

}
