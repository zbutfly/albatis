package net.butfly.albatis.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import net.butfly.albatis.io.OddInput;

public class JdbcInput extends net.butfly.albacore.base.Namedly implements OddInput<Map<String, Object>> {
	private final Connection conn;
	private final PreparedStatement stat;

	private ResultSet rs;

	// resultset infomation
	private boolean next;
	private ResultSetMetaData meta;
	private int colCount;
	private String[] colNames;

	public JdbcInput(final String name, String jdbc, final int batch, final String sql, final Object... params) throws IOException,
			SQLException {
		this(name, DriverManager.getConnection(jdbc), batch, sql, params);
	}

	public JdbcInput(final String name, Connection conn, final int batch, final String sql, final Object... params) throws IOException,
			SQLException {
		super(name);
		logger().debug("[" + name + "] from [" + conn.getClientInfo() + "], sql: \n\t" + sql);
		this.conn = conn;
		// this.conn.setAutoCommit(true);
		this.conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		logger().debug("[" + name + "] query begin...");
		stat = conn.prepareStatement(sql);
		stat.setFetchDirection(ResultSet.FETCH_FORWARD);

		int i = 1;
		for (Object p : params)
			stat.setObject(i++, p);
		batch(100);
		opening(this::openJdbc);
		closing(this::closeJdbc);
		open();
	}

	private void parseMeta() throws SQLException {
		meta = rs.getMetaData();
		colCount = meta.getColumnCount();
		colNames = new String[colCount];
		for (int i = 0; i < colCount; i++) {
			int j = i + 1;
			colNames[i] = meta.getColumnLabel(j);
			int ctype = meta.getColumnType(j);
			logger().debug("ResultSet col[" + j + ":" + colNames[i] + "]: " + meta.getColumnClassName(j) + "/" + meta.getColumnTypeName(j)
					+ "[" + ctype + "]");
		}
	}

	private void openJdbc() {
		long now = System.currentTimeMillis();
		try {
			rs = stat.executeQuery();
			logger().debug("Query spent: " + (System.currentTimeMillis() - now) + " ms.");
			next = rs.next();
			parseMeta();
		} catch (SQLException e) {
			logger().error("SQL Error on JDBC opening", e);
			rs = null;
			next = false;
		}
	}

	private void closeJdbc() {
		try {
			rs.close();
		} catch (SQLException e) {}
		try {
			stat.close();
		} catch (SQLException e) {}
		try {
			conn.close();
		} catch (SQLException e) {}
	}

	@Override
	public boolean empty() {
		return !next;
	}

	private final ReentrantLock lock = new ReentrantLock();

	@Override
	public Map<String, Object> dequeue() {
		if (!next || !lock.tryLock()) return null;
		try {
			Map<String, Object> r = new HashMap<>();
			try {
				for (int i = 0; i < colCount; i++) {
					Object v;
					try {
						v = rs.getObject(i + 1);
					} catch (SQLException e) {
						logger().error("SQL Error on JDBC column fetching", e);
						v = null;
					}
					r.put(colNames[i], v);
				}
				return r;
			} finally {
				try {
					next = rs.next();
				} catch (SQLException e) {
					logger().error("SQL Error on JDBC row nexting", e);
					next = false;
				}
			}
		} finally {
			lock.unlock();
		}
	}

	private JdbcInput batch(int batching) {
		try {
			stat.setFetchSize(batching);
		} catch (SQLException e) {
			logger().error("SQL Error on JDBC fetching size setting", e);
		}
		return this;
	}
}
