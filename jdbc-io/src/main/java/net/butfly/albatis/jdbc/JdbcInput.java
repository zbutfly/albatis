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

import net.butfly.albacore.io.InputImpl;

public class JdbcInput extends InputImpl<Map<String, Object>> {
	private final Connection conn;
	private final PreparedStatement stat;

	private ResultSet rs;

	// resultset infomation
	private boolean next;
	private ResultSetMetaData meta;
	private int colCount;
	private String[] colNames;

	public JdbcInput(final String name, String jdbc, final String sql, final Object... params) throws IOException, SQLException {
		this(name, DriverManager.getConnection(jdbc), sql, params);
	}

	public JdbcInput(final String name, Connection conn, final String sql, final Object... params) throws IOException, SQLException {
		super(name);
		logger().info("[" + name + "] from [" + conn.getClientInfo() + "], sql: \n\t" + sql + "]");
		this.conn = conn;
		// this.conn.setAutoCommit(true);
		this.conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		logger().debug("[" + name + "] query begin...");
		stat = conn.prepareStatement(sql);
		stat.setFetchDirection(ResultSet.FETCH_FORWARD);

		int i = 1;
		for (Object p : params)
			stat.setObject(i++, p);
		opening(this::openJdbc);
		closing(this::closeJdbc);
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
		try {
			rs = stat.executeQuery();
			next = rs.next();
			parseMeta();
		} catch (SQLException e) {
			logger().error("SQL Error", e);
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
	protected Map<String, Object> dequeue() {
		if (!next || !lock.tryLock()) return null;
		try {
			Map<String, Object> r = new HashMap<>();
			try {
				for (int i = 0; i < colCount; i++) {
					Object v;
					try {
						v = rs.getObject(i + 1);
					} catch (SQLException e) {
						v = null;
					}
					r.put(colNames[i], v);
				}
				return r;
			} finally {
				try {
					next = rs.next();
				} catch (SQLException e) {
					next = false;
				}
			}
		} finally {
			lock.unlock();
		}
	}

	public JdbcInput batch(int batching) {
		try {
			stat.setFetchSize(batching);
		} catch (SQLException e) {
			logger().error("SQL Error", e);
		}
		return this;
	}
}
