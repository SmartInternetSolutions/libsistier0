package com.sis.system;

import java.util.Date;

import org.apache.log4j.Logger;

/**
 * A simple profiler.
 * 
 * Example: <code>
 *  Profiler profiler = new Profiler("My routine", 100);
 *  
 *  try {
 *    profiler.start();
 *    
 *    ... your code to profile ...
 *  } finally {
 *    profiler.stop();
 *  }
 * </code>
 * 
 * @author CR
 *
 */
public class Profiler {
	private static final Logger logger = Logger.getLogger(Profiler.class);
	private static boolean verbose = false;
	
	private Date startTime, endTime;
	private String name = "anonymous profiler";
	
	private long autowarnThreshold = 0;
	
	public Profiler() {
	}
		
	public Profiler(String name) {
		this.name = name;  
	}
	
	public Profiler(String name, long autowarnThreshold) {
		this.name = name;  
		this.autowarnThreshold = autowarnThreshold;
	}
	
//	public Profiler(Object that, String methodName) {
//		this(that.getClass().getName() + "#" + methodName);
//	}
//	
//	public Profiler(Object that, String methodName, long autowarnThreshold) {
//		this(that.getClass().getName() + "#" + methodName, autowarnThreshold);
//	}

	public void reset() {
		startTime = null;
		endTime = null;
	}
	
	public void start() {
		startTime = new Date();
		
		if (verbose) {
			logger.debug("started profiling " + getName());
		}
	}
	
	public void stop() {
		endTime = new Date();
		
		long diff = get();
		
		if (verbose) {
			logger.debug("stopped profiling " + getName() + ", took " + diff + "ms.");
		}
		
		if (autowarnThreshold > 0 && diff > autowarnThreshold) {
			logger.warn(getName() + " took longer than " + autowarnThreshold + "ms, took " + diff + "ms!");
		}
	}
	
	public synchronized long stopAndGet() {
		stop();
		
		return get();
	}
	
	public long get() {
		if (startTime == null) {
			logger.warn("uninitialized profiler " + getName());
			
			return -1;
		}
		
		if (endTime == null) {
			stop();
		}
		
		return endTime.getTime() - startTime.getTime();
	}
	
	public String getName() {
		return name;
	}

	public static boolean isVerbose() {
		return verbose;
	}

	public static void setVerbose(boolean verbose) {
		Profiler.verbose = verbose;
	}
}
