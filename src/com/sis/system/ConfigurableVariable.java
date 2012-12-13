package com.sis.system;

import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import com.sis.system.Base;
import com.sis.util.Callback;

/**
 * ConfigurableVariable automatically gets its value from different sources like 
 *  - config.xml
 *  - start-up parameter
 *  - by command (e.g. issued by Thrift interface)
 * 
 * You can listen to changes by setting a callback via {@link ConfigurableVariable#setRefreshCallback(Callback)}.
 * 
 * @author CR
 *
 * @param <T>
 */
public final class ConfigurableVariable<T> {
	protected T value = null, defaultValue = null;
	protected String configPath = null, optionName = null, argument = null;
	protected Callback refreshCallback = null;
	protected boolean uninitialized = true;

	private static final Logger logger = Logger.getLogger(ConfigurableVariable.class);
	
	private static final HashSet<ConfigurableVariable<?>> knownConfigVars = new HashSet<ConfigurableVariable<?>>();
	
	public static void debugShow() {
		System.out.println("=== Configurable Variables ===");

		for (ConfigurableVariable<?> configVar : knownConfigVars) {
			Object obj = configVar.getValue();
			System.out.printf("%-40s %s\n", configVar.getConfigPath(), obj == null ? "<null>" : obj.toString());
		}
	}
	
	public static ConfigurableVariable<?> getConfigurableVariableByConfigPath(String configPath) {
		for (ConfigurableVariable<?> configVar : knownConfigVars) {
			if (configVar.getConfigPath() == configPath) {
				return configVar;
			}
		}
		
		return null;
	}
	
	public static ConfigurableVariable<?> getConfigurableVariableByOptionName(String optionName) {
		for (ConfigurableVariable<?> configVar : knownConfigVars) {
			if (configVar.getOptionName() == optionName) {
				return configVar;
			}
		}
		
		return null;
	}
	
	public static HashSet<ConfigurableVariable<?>> getConfigurableVariables() {
		return knownConfigVars;
	}
	
	public static boolean trySetConfigurableVariableByOptionName(String optionName, String val) {
		ConfigurableVariable<?> cfgvar = getConfigurableVariableByOptionName(optionName);
		
		if (cfgvar == null) {
			return false;
		}
		
		cfgvar.setValueFromString(val);
		
		return true;
	}
	
	public static boolean trySetConfigurableVariableByConfigPath(String configPath, String val) {
		ConfigurableVariable<?> cfgvar = getConfigurableVariableByConfigPath(configPath);
		
		if (cfgvar == null) {
			return false;
		}
		
		cfgvar.setValueFromString(val);
		
		return true;
	}
	
	public ConfigurableVariable(String configPath, T defaultValue, String optionName) {
		prepare(configPath, defaultValue);
		
		this.optionName = optionName;
	}
	
	public ConfigurableVariable(String configPath, T defaultValue, String optionName, String argument) {
		prepare(configPath, defaultValue);

		this.optionName = optionName;
		this.argument = argument;
		
		if (Base.hasArgument(argument)) {
			String value = Base.getArgument(argument);
			setValueFromString(value);
			logger.debug("variable " + configPath + " set by argument " + argument + " to " + value);
		} // CR: doesn't make sense because usually this is called before Base.init in main...
	}
	
	public ConfigurableVariable(String configPath, T defaultValue) {
		prepare(configPath, defaultValue);
	}

	protected void prepare(String configPath, T defaultValue) {
		this.configPath = configPath;
		this.value = defaultValue;
		this.defaultValue = defaultValue;
		
		knownConfigVars.add(this);
	}
		
	public T getValue() {
		if (uninitialized) {
			if (argument != null && Base.hasArgument(argument)) {
				setValueFromString(Base.getArgument(argument));
			} else if (XmlConfig.getSingleton().hasValue(configPath)) {
				try {
					String val = XmlConfig.getSingleton().getValue(configPath);
					
					if (val != null && !val.isEmpty()) {
						setValueFromString(val);
					}
				} catch(Exception e) {
					logger.warn("failed to initialize variable " + configPath, e);
				}
				
				if (value == null) {
					value = defaultValue;
					
					logger.debug("reset variable " + configPath + " to default value " + defaultValue);
				}
			}
			
			uninitialized = false;
			
			logger.trace("variable " + configPath + " initialized.");
		}
		
		return value;
	}
	
	public void setValue(T t) {
		value = t;
	}

	@SuppressWarnings("unchecked")
	private synchronized boolean setValueFromObject(Object object) {
		boolean hasChanged = false, notHandled = false;
	
		if (object instanceof String) {
			String string = (String) object;
			
			if (defaultValue instanceof Integer) {
				Integer newValue = Integer.parseInt(string); // Integer.getInteger(string); 
				
				if (value != null && !((Integer) value).equals(newValue)) {
					hasChanged = true;
				}
	
				value = (T) newValue;
			} else if (defaultValue instanceof Double) {
				Double newValue = Double.valueOf(string); 
				
				if (value != null && !((Double) value).equals(newValue)) {
					hasChanged = true;
				}
				
				value = (T) newValue;
			} else if (defaultValue instanceof Boolean) {
				Boolean newValue = Boolean.getBoolean(string); 
				
				if (value != null && !((Boolean) value).equals(newValue)) {
					hasChanged = true;
				}
				
				value = (T) newValue;
			} else if (defaultValue instanceof String) {
				if (value != null && !((String) value).equals(string)) {
					hasChanged = true;
				}
				
				value = (T) string;
			} else {
				notHandled = true;
			}
		} else if (object instanceof List) {
			List<?> listValue = (List<?>) value;
			
			if (value != null && !(listValue).equals(object)) {
				hasChanged = true;
			}
			
			listValue.clear();
			// TODO
			
		} else {
			notHandled = true;
		}
		
		if (notHandled) {
			logger.fatal("unknown T given for variable " + configPath);
			throw new RuntimeException("T is no Boolean, Double, Integer or String. Please fix this.");
		}
		
		return hasChanged;
	}
	
	public synchronized void setValueFromString(String string) { // ugly code is ugly
		if (setValueFromObject(string) && !uninitialized) {
			if (refreshCallback != null) {
				refreshCallback.call();
			}
		}

		if (value == null) {
			value = defaultValue;
		}
	}
	
	public synchronized void setValueFromList(List<String> list) {
		if (setValueFromObject(list) && !uninitialized) {
			if (refreshCallback != null) {
				refreshCallback.call();
			}
		}

		if (value == null) {
			value = defaultValue;
		}
	}

	/**
	 * this callback will be called as soon as the value has been changed after it was initialized once
	 * @param refreshCallback
	 */
	public void setRefreshCallback(Callback refreshCallback) {
		this.refreshCallback = refreshCallback;
	}

	public synchronized void reset() {
		uninitialized = true;
	}

	public String getConfigPath() {
		return configPath;
	}

	public String getOptionName() {
		return optionName;
	}

	public static synchronized void resetAll() {
		for (ConfigurableVariable<?> cfgVar : knownConfigVars) {
			cfgVar.reset();
		}
	}
	
	public String toString() {
		return getValue().toString();
	}
}
