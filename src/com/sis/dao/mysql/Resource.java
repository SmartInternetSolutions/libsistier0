package com.sis.dao.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.sis.dao.Collection;
import com.sis.dao.DaoManager;
import com.sis.dao.DaoException;
import com.sis.dao.DataObject;
import com.sis.dao.DataObject.DbExpression;
import com.sis.system.Profiler;
import com.sis.system.XmlConfig;
import com.sis.util.Pair;
import com.sis.util.StringHelper;

/**
 * 
 * @todo transactions are NOT thread safe!
 * 
 * @author CR
 *
 */
public class Resource implements com.sis.dao.Resource {
	private static final transient Logger logger = Logger.getLogger("MySQL");
	
	/**
	 * helper class for join statements
	 * @author CR
	 *
	 */
	static class JoinStatement {
		public static final int INNER = 0;
		public static final int LEFT  = 1;
		
		public static final String types2sql[] = {"", "LEFT"};
		
		private int joinType = INNER;
		
		private String tablename, expression;
		
		private final HashSet<String> columns = new HashSet<String> (); 
		
		public JoinStatement(String tablename, String expression, int type) {
			this.tablename = tablename;
			this.expression = expression;
			this.joinType = type;
		}
		
		public JoinStatement(String tablename, String expression, int type, String columns[]) {
			this(tablename, expression, type);
			
			for (int i = 0; i < columns.length; i++) {
				this.columns.add(columns[i]);
			}
		}
		
		public JoinStatement(String field, String expression) {
			this(field, expression, INNER);
		}

		public int getJoinType() {
			return joinType;
		}

		public void setJoinType(int joinType) {
			this.joinType = joinType;
		}

		public String getTablename() {
			return tablename;
		}

		public void setTablename(String tablename) {
			this.tablename = tablename;
		}

		public String getExpression() {
			return expression;
		}

		public void setExpression(String expression) {
			this.expression = expression;
		}

		public String getJoinTypeInSQL() {
			return types2sql[getJoinType()];
		}

		public String getColumns() {
			return columns.size() > 0 ? StringHelper.coalesce(columns, ',') : "*";
		}
	}
	
	protected static Map<String, Connection> conCache = new ConcurrentHashMap<String, Connection>();

	public static synchronized void closeAll() {
		for (Connection connection : conCache.values()) {
			try {
				connection.close();
			} catch (SQLException e) {
				logger.error("connection could not be closed!", e);
			}
		}

		conCache.clear();
	}

    protected String idFieldName 	= "id";

	protected String tableName		= "";

//	private String shardedBy 	= null;
//	private String shardSuffix 	= "unknown";

	protected static Connection readConnections[]	= new Connection[12];
	protected static Connection writeConnections[]	= new Connection[4];
	
	protected static final ConcurrentHashMap<Thread, Integer> writeConnectionOwnerships = new ConcurrentHashMap<>();
	
	protected static void releaseOwnershipForWriteConnectionByCurrentThread() {
		writeConnectionOwnerships.remove(Thread.currentThread());
	}

	protected static synchronized void takeOwnershipForWriteConnectionByCurrentThread() {
		if (!hasCurrentThreadOwnershipForWriteConnection()) {
			writeConnectionOwnerships.put(Thread.currentThread(), (int) Math.round(Math.random() * writeConnections.length));
		}
	}
	
	protected static boolean hasCurrentThreadOwnershipForWriteConnection() {
		return writeConnectionOwnerships.contains(Thread.currentThread());
	}
	
	protected static synchronized Connection getReadConnection() {
		int candidate = (int) Math.floor(Math.random() * readConnections.length);
		
		if (readConnections[candidate] == null) {
			readConnections[candidate] = getConnection("slave");
			
			logger.debug("set up new slave connection to candidate " + candidate + ".");
		}

		return readConnections[candidate];
	}

	protected static synchronized Connection getWriteConnection() {

		int candidate = 0;
		
		if (hasCurrentThreadOwnershipForWriteConnection()) {
			candidate = writeConnectionOwnerships.get(Thread.currentThread());
		} else {
			candidate = (int) Math.floor(Math.random() * writeConnections.length);
		}
		
		if (writeConnections[candidate] == null) {
			writeConnections[candidate] = getConnection("master");

			logger.debug("set up new master connection to candidate " + candidate + ".");
		}

		return writeConnections[candidate];
	}

	protected static Connection getConnection(String dbId) {
		Connection con = null;

		// fill cache
		if (!conCache.containsKey(dbId)) {
			String url = XmlConfig.getSingleton().getValue("mysql/" + dbId + "/url");
			String username = XmlConfig.getSingleton().getValue("mysql/" + dbId + "/username");
			String password = XmlConfig.getSingleton().getValue("mysql/" + dbId + "/password");

			try {
				con = DriverManager.getConnection(url, username, password);
				
				logger.debug("created MySQL connection for " + dbId + " on " + url);
			} catch (SQLException e) {
				logger.error("connection could not be created!", e);
			}

			conCache.put(dbId, con);
		} else {
			con = conCache.get(dbId);
		}

		return con;
	}

	public static Connection renewConnection(String dbId)
	{
		Connection connection;

		synchronized (conCache) {
			conCache.remove(dbId);
			connection = getConnection(dbId);
		}

		return connection;
	}

	public static Connection renewConnection(Connection dbId)
	{
		Connection connection = null;

		for (String key : conCache.keySet()) {
			if (dbId == conCache.get(key)) {
				synchronized (conCache) {
					conCache.remove(key);
					connection = getConnection(key);
				}
			}
		}

		return connection;
	}

    public String getIdFieldName() {
		return idFieldName;
	}

	public void setIdFieldName(String idFieldName) {
		this.idFieldName = idFieldName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public int getCapabilities() {
		return 	CAPABILITY_UPDATE |
				CAPABILITY_DELETE |
				CAPABILITY_PRIMARYKEY |
				CAPABILITY_INSERT;
	}

	private String createTableName() {
//		if (shardedBy != null) {
//			return "`" + tableName + "_" + shardSuffix + "`"; // "`" + + "`.`" + "`";
//		}

		return "`" + tableName + "`";
	}

	@Override
	public void load(DataObject object, String value, String field) throws DaoException {
		Profiler profiler = new Profiler("MySQL load Object", DaoManager.getDaoExecutionWarningThreshold());
		profiler.start();
		
		Connection connection = getReadConnection();
		
		synchronized (connection) {
			try {
				PreparedStatement stmt = connection.prepareStatement("SELECT * FROM `" + tableName + "` WHERE `" + field + "` = ? LIMIT 1");
	
				stmt.setString(1, value);
	
				stmt.setMaxRows(1);
	
				ResultSet res = null;
	
				if (logger.isDebugEnabled()) {
					logger.debug("loading object via " + stmt.toString());
				}
				
				try {
					res = stmt.executeQuery();
				} catch(SQLRecoverableException e) {
					logger.debug("first executeQuery() failed, ignoring.", e);
					res = stmt.executeQuery();
				}
	
				ResultSetMetaData rsmd = res.getMetaData();
			    int numColumns = rsmd.getColumnCount();
	
				if (res.next()) {
					for (int i=1; i < numColumns + 1; i++) {
				        String columnName = rsmd.getColumnName(i);
	
				        object.setData(columnName, res.getObject(columnName));
				    }
				}
	
				res.close();
				stmt.close();
			} catch (SQLException e) {
				throw new DaoException("unable to load data", e);
			} finally {
				profiler.stop();
			}
		}
	}

	@Override
	public String insert(Map<String, java.lang.Object> fields) throws DaoException {
		Profiler profiler = new Profiler("MySQL insert Object", DaoManager.getDaoExecutionWarningThreshold());
		profiler.start();
		
//		if (shardedBy != null && fields.containsKey(shardedBy)) {
//			shardSuffix = fields.get(shardedBy).toString();
//		}

		String sql = "INSERT INTO " + createTableName() + " SET ";

		Connection connection = getWriteConnection();
		
		synchronized (connection) {
			try {
				for (Map.Entry<String, Object> entry : fields.entrySet()) {
					Object val = entry.getValue();
					String key = entry.getKey();
					
					if (val instanceof DbExpression) {
						sql += "`" + key + "` = " + val.toString();
					} else {
						sql += "`" + key + "` = ?";
					}
					
					sql += ", ";
				}
	
				sql = sql.substring(0, sql.length() - 2);
	
				PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
	
				int field = 1;
				for (String key : fields.keySet()) {
					Object obj = fields.get(key);
					
					if (!(obj instanceof DbExpression)) {
						stmt.setObject(field++, obj);
					}
				}
				
				if (logger.isDebugEnabled()) {
					logger.debug("inserting object via " + stmt.toString());
				}
				
				try {
					stmt.executeUpdate();
				} catch(SQLRecoverableException e) {
					logger.debug("first executeUpdate() failed, ignoring.", e);
					stmt.executeUpdate();
				}
	
				String id = "";
				ResultSet rs = stmt.getGeneratedKeys();
	
				if (rs.next()) {
		            id = rs.getString(1);
		        }
	
		        rs.close();
				stmt.close();
	
		        return id;
			} catch (SQLException e) {
				e.printStackTrace();
				throw new DaoException("unable to insert data", e);
			} finally {
				profiler.stop();
			}
		}
	}

	@Override
	public boolean update(String id, Map<String, java.lang.Object> updateMap) throws DaoException {
		Profiler profiler = new Profiler("MySQL update Object", DaoManager.getDaoExecutionWarningThreshold());
		profiler.start();

//		if (shardedBy != null && updateMap.containsKey(shardedBy)) {
//			shardSuffix = updateMap.get(shardedBy).toString();
//		}

		String sql = "UPDATE " + createTableName() + " SET ";

		Connection connection = getWriteConnection();
		
		synchronized (connection) {
			try {
				for (Map.Entry<String, Object> entry : updateMap.entrySet()) {
					Object val = entry.getValue();
					String key = entry.getKey();
					
					if (val instanceof DbExpression) {
						sql += "`" + key + "` = " + val.toString();
					} else {
						sql += "`" + key + "` = ?";
					}
					
					sql += ", ";
				}
	
				sql = sql.substring(0, sql.length() - 2) + " WHERE `" + idFieldName +  "` = ? LIMIT 1";
	
				PreparedStatement stmt = connection.prepareStatement(sql);
	
				int field = 1;
				for (String key : updateMap.keySet()) {
					Object obj = updateMap.get(key);
					
					if (!(obj instanceof DbExpression)) {
						stmt.setObject(field++, obj);
					}
				}
	
				stmt.setString(field++, id);
	
				if (logger.isDebugEnabled()) {
					logger.debug("updating object via " + stmt.toString());
				}
				
				try {
					stmt.executeUpdate();
				} catch(SQLRecoverableException e) {
					logger.debug("first executeUpdate() failed, ignoring.", e);
					stmt.executeUpdate();
				}
				
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
				throw new DaoException("unable to update data", e);
			} finally {
				profiler.stop();
			}
		}

		return true;
	}

	@Override
	public boolean delete(String id) throws DaoException {
		Profiler profiler = new Profiler("MySQL delete Object", DaoManager.getDaoExecutionWarningThreshold());
		profiler.start();

		Connection connection = getWriteConnection();
		
		synchronized (connection) {
			try {
				PreparedStatement stmt = connection.prepareStatement("DELETE FROM " + createTableName() + " WHERE `" + idFieldName + "` = ?");
				stmt.setObject(1, id);
	
				if (logger.isDebugEnabled()) {
					logger.debug("deleting object via " + stmt.toString());
				}
				
				try {
					stmt.executeUpdate();
				} catch(SQLRecoverableException e) {
					logger.debug("first executeUpdate() failed, ignoring.", e);
					stmt.executeUpdate();
				}
				
				stmt.close();
			} catch(SQLException e) {
				throw new DaoException("unable to delete data", e);
			} finally {
				profiler.stop();
			}
		}

		return false;
	}

	@Override
	public void beginTransaction() throws DaoException {
		logger.trace("beginTransaction()");
		
		try {
			takeOwnershipForWriteConnectionByCurrentThread();
			
			getWriteConnection().setAutoCommit(false);
		} catch (SQLException e) {
			throw new DaoException("beginTransaction failed, " + e.getMessage(), e);
		}
	}

	@Override
	public void commit() throws DaoException {
		logger.trace("commit()");
		
		try {
			Connection connection = getWriteConnection();
			
			synchronized (connection) {
				connection.commit();
				connection.setAutoCommit(true);	
			}
			
			releaseOwnershipForWriteConnectionByCurrentThread();
		} catch (SQLException e) {
			throw new DaoException("commit failed, " + e.getMessage(), e);
		}
	}
	
	@Override
	public void rollback() throws DaoException {
		logger.trace("rollback()");
		
		try {
			Connection connection = getWriteConnection();
			
			synchronized (connection) {
				connection.rollback();
				connection.setAutoCommit(true);
			}
			
			releaseOwnershipForWriteConnectionByCurrentThread();
		} catch (SQLException e) {
			throw new DaoException("rollback failed, " + e.getMessage(), e);
		}
	}

	@Override
	public void loadCollection(Collection collection, java.lang.Object args[]) throws DaoException {
	}

	final private static String[] filter2sql = new String[] {
		"",
		"=",
		"LIKE",
		"<",
		"<=",
		">",
		">=",
		"!=",
		"",
		""
	};
	
	final private static String[] filter2sql4null = new String[] {
		"",
		"IS",
		"IS",
		"IS NOT",
		"IS",
		"IS NOT",
		"IS",
		"IS NOT",
		"",
		""
	};

	protected String compileSelectStatement() {
		Profiler profiler = new Profiler("MySQL compile SELECT", 2);
		profiler.start();
		
		try {
			HashSet<String> sets = new HashSet<String> ();
			
			sets.add(createTableName() + ".*");
			
			if (joinStatements.size() > 0) {
				for (JoinStatement stmt : joinStatements.values()) {
					sets.add("`" + stmt.getTablename() + "`." + stmt.getColumns());
				}
			}
			
			return "SELECT " + StringHelper.coalesce(sets, ',');
		} finally {
			profiler.stop();
		}
	}

	protected String compileWhere(HashMap<String, Pair<java.lang.Object[], Integer>> fieldFilters) {
		Profiler profiler = new Profiler("MySQL compile WHERE", 5);
		profiler.start();
		
		try {
			StringBuilder sql = new StringBuilder();
			
			Iterator<Map.Entry<String, Pair<java.lang.Object[], Integer>>> it = fieldFilters.entrySet().iterator();
			
			if (it.hasNext()) {
				sql.append("WHERE ");
			}
			
			while (it.hasNext()) {
				Map.Entry<String, Pair<java.lang.Object[], Integer>> pair = it.next();
				
				String field = pair.getKey();
				Pair<java.lang.Object[], Integer> dpair = pair.getValue();
				java.lang.Object[] values = dpair.getLeft();
				
				if (dpair.getRight() == Collection.FILTER_CUSTOM) {
					if (values.length > 1) {
						for (int i = 0, j = values.length; i < j; i++) {
							if (i > 0) {
								sql.append(" OR ");
							}

							sql.append('(');
							sql.append(values[i]);
							sql.append(')');
						}
						
					} else {
						sql.append(field);
						sql.append(' ');
					}					
				} else {
					
					sql.append('(');
					
					for (int i = 0, j = values.length; i < j; i++) {
						// escape field
						
						if (i > 0) {
							sql.append(" OR ");
						}
						
						sql.append('`');
						sql.append(field.replace(".", "`.`"));
						sql.append('`');
						
						if (values[i] == null) {
							sql.append(filter2sql4null[dpair.getRight()]);
						} else {
							sql.append(filter2sql[dpair.getRight()]);
						}
						
						sql.append(" ?");
					}
					
					sql.append(')');
				}
	
				if (it.hasNext()) {
					sql.append(" AND ");
				}
			}
			
			return sql.toString();
		} finally {
			profiler.stop();
		}
	}

	final private static String[] sort2sql = new String[] {
		"",
		"ASC",
		"DESC"
	};
	
	public void loadCollection(Collection collection,
			HashMap<String, Pair<java.lang.Object[], Integer>> fieldFilters,
			long limitCount, long limitOffset, HashMap<String, Integer> fieldSortings) throws DaoException {
		Profiler profiler = new Profiler("MySQL load Collection", DaoManager.getDaoExecutionWarningThreshold());
		profiler.start();
		
		try {
			StringBuilder sql = new StringBuilder();
			sql.append(compileSelectStatement());  // TODO: add field selection capability, TODO respect sharding
			sql.append(" FROM ");
			sql.append(createTableName());
			sql.append(' ');
			sql.append(compileJoin());
			sql.append(' ');
			sql.append(compileWhere(fieldFilters));
			
			if (fieldSortings.size() > 0) {
				sql.append("ORDER BY ");
				
				Iterator<Map.Entry<String, Integer>> it = fieldSortings.entrySet().iterator();
				
				while (it.hasNext()) {
					Map.Entry<String, Integer> pair = it.next();
					
					sql.append(pair.getKey());
					sql.append(' ');
					sql.append(sort2sql[pair.getValue()]);
					
					if (it.hasNext()) {
						sql.append(',');
					}
				}
			}
			
			if (limitCount > 0) {
				sql.append("LIMIT ");
				sql.append(limitOffset);
				sql.append(',');
				sql.append(limitCount);
			}
			
			String csql = null;
			
			Connection connection = getWriteConnection();
			
			synchronized (connection) {
				try {
					PreparedStatement stmt = connection.prepareStatement(sql.toString());
		
					int counter = 1;
//					for (String key : fieldFilters.keySet()) { // FIXME: optimize
					for (Entry<String, Pair<Object[], Integer>> entry : fieldFilters.entrySet()) {
						java.lang.Object[] objects = entry.getValue().getLeft();
		
						for (int i = 0, j = objects.length; i != j; i++) {
							if (entry.getValue().getRight() == Collection.FILTER_CUSTOM && j > 1) {
								stmt.setObject(counter++, entry.getKey());
							} else  {
								stmt.setObject(counter++, objects[i]);
							}
						}
					}
					
					if (logger.isDebugEnabled()) {
						logger.debug("loading collection via " + stmt.toString());
					}
					
					csql = stmt.toString();
		
					ResultSet res = null;
		
					try {
						res = stmt.executeQuery();
					} catch(SQLRecoverableException e) {
						logger.debug("first executeQuery() failed, ignoring.", e);
						res = stmt.executeQuery();
					}
		
					while (res.next()) {
						com.sis.dao.mysql.DataObject item = (com.sis.dao.mysql.DataObject) collection.newItem();
		
						ResultSetMetaData rsmd = res.getMetaData();
					    int numColumns = rsmd.getColumnCount();
		
						for (int i=1; i < numColumns + 1; i++) {
					        String columnName = rsmd.getColumnName(i);
		
					        item.setData(columnName, res.getObject(columnName));
					    }
		
						collection.addItem(item);
					}
		
					res.close();
					stmt.close();
		
				} catch (SQLException e) {
					
					throw new DaoException("unable to select: " + e.getMessage() + ", sql: " + (csql != null ? csql : "<null>"), e);
				}
				
			}
		} finally {
			profiler.stop();
		}
	}
	
	public void setShardedBy(String fieldName) {
		logger.trace("resource shared by " + fieldName);
//		this.shardedBy  = fieldName;
	}

	public long count(HashMap<String, Pair<java.lang.Object[], Integer>> fieldFilters, long limitCount, long limitOffset) {
		Profiler profiler = new Profiler("MySQL count Collection", DaoManager.getDaoExecutionWarningThreshold());
		profiler.start();
		
		long l = 0;

		try {
			String sql = "SELECT COUNT(*) FROM " + createTableName() + " " + compileJoin() + " " + compileWhere(fieldFilters);

			PreparedStatement stmt = getWriteConnection().prepareStatement(sql);

			int counter = 1;
			for (String key : fieldFilters.keySet()) {
				java.lang.Object[] objects = fieldFilters.get(key).getLeft();

				for (int i = 0, j = objects.length; i != j; i++) {
					stmt.setObject(counter++, objects[i]);
				}
			}

			ResultSet res = null;

			if (logger.isDebugEnabled()) {
				logger.debug("counting via " + stmt.toString());
			}
			
			try {
				res = stmt.executeQuery();
			} catch(SQLRecoverableException e) {
				logger.debug("first executeQuery() failed, ignoring.", e);
				res = stmt.executeQuery();
			}

			if (res.next()) {
				l = res.getLong(1);
			}

			res.close();
			stmt.close();

		} catch (SQLException e) {
			logger.error("counting from database failed!", e);
		} finally {
			profiler.stop();
		}

		return l;
	}
	
	HashMap<String, JoinStatement> joinStatements = new HashMap<String, JoinStatement> ();
	
	private String compileJoin() {
		StringBuilder sb = new StringBuilder();

		for (JoinStatement stmt : joinStatements.values()) {
			sb.append(stmt.getJoinTypeInSQL());
			sb.append(" JOIN `");
			sb.append(stmt.getTablename());
			sb.append('`');
			sb.append(" ON (");
			sb.append(stmt.getExpression());
			sb.append(')');
		}

		return sb.toString();
	}
	
	public void addJoinStatement(String tablename, String expression, String columns[]) {
		joinStatements.put(tablename, new JoinStatement(tablename, expression, JoinStatement.INNER, columns));
	}
	
	public void addLeftJoinStatement(String tablename, String expression, String columns[]) {
		joinStatements.put(tablename, new JoinStatement(tablename, expression, JoinStatement.LEFT, columns));
	}
	
	public void addJoinStatement(String tablename, String expression) {
		joinStatements.put(tablename, new JoinStatement(tablename, expression, JoinStatement.INNER));
	}
	
	public void addLeftJoinStatement(String tablename, String expression) {
		joinStatements.put(tablename, new JoinStatement(tablename, expression, JoinStatement.LEFT));
	}
	
	public static void rawWrite(String definitionStatement) throws DaoException {
		try {
			PreparedStatement stmt = getWriteConnection().prepareStatement(definitionStatement, Statement.RETURN_GENERATED_KEYS);
			
			stmt.execute();
		} catch (SQLException e) {
			throw new DaoException("raw write failed", e);
		}
	}
}
