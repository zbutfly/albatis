package net.butfly.albatis.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import net.butfly.albacore.io.InputImpl;
import net.butfly.albacore.utils.logger.Logger;

public class JdbcInput extends InputImpl<ResultSet> {
	private static final Logger logger = Logger.getLogger(JdbcInput.class);
	private final Connection conn;
	private final PreparedStatement stat;
	private ResultSet rs;
	private boolean rsNext;

	public JdbcInput(final String name, String jdbc, final String sql, final Object... params) throws IOException, SQLException {
		this(name, DriverManager.getConnection(jdbc), sql, params);
	}

	public JdbcInput(final String name, Connection conn, final String sql, final Object... params) throws IOException, SQLException {
		super(name);
		logger.info("[" + name + "] from [" + conn.getClientInfo() + "], sql: \n\t" + sql + "]");
		this.conn = conn;
		// this.conn.setAutoCommit(true);
		this.conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		logger.debug("[" + name + "] query begin...");
		stat = conn.prepareStatement(sql);
		stat.setFetchDirection(ResultSet.FETCH_FORWARD);

		int i = 1;
		for (Object p : params)
			stat.setObject(i++, p);
		opening(() -> {
			try {
				rs = stat.executeQuery();
				rsNext = rs.next();
			} catch (SQLException e) {
				logger().error("SQL Error", e);
				rs = null;
				rsNext = false;
			}
		});
		closing(this::closeJdbc);
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
		return !rsNext;
	}

	@Override
	protected ResultSet dequeue() {
		return rs;
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
