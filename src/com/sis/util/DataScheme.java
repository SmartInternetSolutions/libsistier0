package com.sis.util;

import javax.xml.bind.DatatypeConverter;

public class DataScheme {
	private byte[] data = new byte[0];
	private String mime = "application/octet-stream";

	public DataScheme(String scheme) {
		parseScheme(scheme);
	}
	
	// data:image/gif;base64,R0
	public void parseScheme(String scheme) {
		StringBuilder mime = new StringBuilder();
		StringBuilder type = new StringBuilder();
		
		if (scheme.startsWith("data:")) {
			int i = 5, j = data.length;
			for (; i != j; i++) {
				char c = scheme.charAt(i);
				
				if (c == ';') {
					i++;
					break;
				}
				
				mime.append(c);
			}
			
			for (; i != j; i++) {
				char c = scheme.charAt(i);
				
				if (c == ',') {
					i++;
					break;
				}
				
				type.append(c);
			}
			
			this.mime = mime.toString();
			
			String _type = type.toString();
			String lexicalXSDBase64Binary = scheme.substring(i);
			if (_type.equalsIgnoreCase("base64")) {
				data = DatatypeConverter.parseBase64Binary(lexicalXSDBase64Binary);
			} else {
				// TODO: exception, more decoders
			}
		} else {
			// TODO: exception
		}
	}
	
	public String getMime() {
		return mime;
	}
	
	public byte[] getData() {
		return data;
	}
	
	public long getLength() {
		return data.length;
	}
}
