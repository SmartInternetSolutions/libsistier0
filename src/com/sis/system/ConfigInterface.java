package com.sis.system;

import java.net.URL;
import java.util.HashMap;

public interface ConfigInterface {
	public void init();
	public void reinitialize();
	public void shutdown();
	
	public boolean loadConfig(URL filename);
	
	public String getValue(String name);
	public boolean hasValue(String name);
	public HashMap<String, String> getValues(String name);
}
