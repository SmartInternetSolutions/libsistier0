package com.sis.dao;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.sis.system.ConfigurableVariable;

public class DaoManager {
	private static final ConfigurableVariable<Integer> daoExecutionWarningThreshold = new ConfigurableVariable<Integer>("dao/execution-threshold/warning", 650, "dao.execution-threshold.warning");
	private static final Logger logger = Logger.getLogger(DaoManager.class);
	
	public static int getDaoExecutionWarningThreshold() {
		return daoExecutionWarningThreshold.getValue();
	}
	
	/**
	 * @todo register daos here, refactor later?
	 * 
	 * @throws SQLException
	 */
	public static void initDrivers() throws SQLException {
		DriverManager.registerDriver(new com.mysql.jdbc.Driver());
		logger.info("dao drivers initialized.");
	}
}
