package com.sis.system;

import java.net.URL;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * {@link Base#init(String[])} should be one of the first lines in every entry point method. 
 * 
 * You can create dynamic variables with {@link ConfigurableVariable}.
 * 
 * Configuration file is located in etc/config.xml.
 * 
 * @author CR
 *
 */
public class Base {	
	private static final Logger logger = Logger.getLogger(Base.class);
	
	private static XmlConfig config;

	private static boolean isShuttingDown;
	
	/**
	 * init parses command line, initializes log4j and prepares {@link ConfigurableVariable}.
	 * 
	 * @note this method should be called directly after a bunch of {@link Base#setArgumentHints(HashMap)}, so it can processes --help correctly.
	 * 
	 * @param args (optional, to be used with jetty or tomcat containment)
	 * @throws Exception
	 */
	public static void init(String[] args) throws Exception {
		DOMConfigurator.configureAndWatch("etc/log4j.xml", 60000);
		
		if (args == null) {
			return;
		}
		
		parseArgumentList(args);
		
		if (hasArgument("-h") || hasArgument("--help")) {
			printHelp();

			System.exit(1);
		}
		
		Profiler.setVerbose(true);
		
		if (hasArgument("--config")) {
			logger.trace("alternative configuration file given, trying to load.");
			
			config = XmlConfig.getSingleton();
			
			if (!config.loadConfig(new URL(getArgument("--config")))) {
				config.init();
			}
		} else {
			config = XmlConfig.getSingleton();
			config.init();
		}
		
		if (hasArgument("--set")) {
			String data = getArgument("--set");
			String[] datas = data.split(";");
			
			for (int i = 0; i < datas.length; i++) {
				String[] parts = datas[i].split("=");
				
				if (parts.length == 2) {
					config.overwriteValue(parts[0], parts[1]);
				}
			}
		}
	}
	
	/**
	 * notifies all threads to shutdown gracefully ({@see Base#isShuttingDown()}
	 */
	public static void shutdown() {
		isShuttingDown = true;
	}
	
	final private static HashMap<String, Object> argumentList = new HashMap<String, Object> ();
	final private static LinkedList<Object> otherStrings = new LinkedList<Object> ();
	
	@SuppressWarnings("serial")
	final private static Map<String, String> argumentHints = new LinkedHashMap<String, String> () {{
		put("--set", "overwrites config statement permanently. e.g. \"config/path=blah;config/name=test\"");
		put("--config", "specifies config path");
	}};

	// "name" 
	// "-key" "value" {key:}
	// "-key1" "-key2" "value" "-key3"
	
	private static void parseArgumentList(String[] args) {
		boolean isInKeyMode = false;
		String lastArg = "";
		
		for (int i = 0, j = args.length; i != j; i++) {
			if (args[i].length() == 0) {
				continue;
			}
			
			String arg = args[i];
			
			// found element is a key
			if (arg.charAt(0) == '-') {
				// is already in key mode
				if (isInKeyMode) { // -lastArg -arg 
					argumentList.put(lastArg, true);
				} else { // -arg
					isInKeyMode = true;
				}
			} else {
				isInKeyMode = false;
				if (lastArg.length() > 0 && lastArg.charAt(0) == '-') {
					argumentList.put(lastArg, arg);
				} else {
					otherStrings.add(arg);
				}
			}
			
			lastArg = arg;
		}
		
		if (isInKeyMode) {
			argumentList.put(lastArg, true);
		}
	}

	public static boolean hasArgument(String arg) {
		return argumentList.containsKey(arg);
	}
	
	public static String getArgument(String arg) {
		return argumentList.get(arg).toString();
	}
	
	public static LinkedList<Object> getOtherArguments() {
		return otherStrings;
	}

	public static void setArgumentHints(HashMap<String, String> hashMap) {
		argumentHints.putAll(hashMap);
	}

	public static void printHelp() {
		System.err.println("known available arguments:");
		
		for (String string : argumentHints.keySet()) {
			System.err.printf("  %-20s %s\n", string, argumentHints.get(string));
		}
	}

	public static boolean isShuttingDown() {
		return isShuttingDown;
	}
}
