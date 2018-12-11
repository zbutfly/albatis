package net.butfly.albatis.jdbc.dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import net.butfly.albatis.io.Rmap;

import net.butfly.albatis.jdbc.dialect.Dialect.DialectFor;

@DialectFor(subSchema = "kingbaseanalyticsdb", jdbcClassname = "com.kingbase.kingbaseanalyticsdb.ds.KBSimpleDataSource")
public class KingbaseDialect extends Dialect {
	private static final String psql = "insert into %s (%s) values(%s)";

	@Override
	protected void doInsertOnUpsert(Connection conn, String t, List<Rmap> l, AtomicLong count) {
		List<Rmap> ml = l.stream().sorted((m1, m2) -> m2.size() - m1.size()).collect(Collectors.toList());
		List<String> fl = new ArrayList<>(ml.get(0).keySet());
		String fields = fl.stream().collect(Collectors.joining(", "));
		String values = fl.stream().map(f -> "?").collect(Collectors.joining(", "));
		String sql = String.format(psql, t, fields, values);
		logger().debug("ORACLE upsert SQL: " + sql);
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ml.forEach(m -> {
				try {
					for (int i = 0; i < fl.size(); i++) {
						Object value = m.get(fl.get(i));
						setObject(ps, i + 1, value);
					}
					ps.addBatch();
				} catch (SQLException e) {
					logger().warn(() -> "add `" + m + "` to batch error, ignore this message and continue.", e);
				}
			});
			int[] rs = ps.executeBatch();
			long sucessed = Arrays.stream(rs).filter(r -> r >= 0).count();
			count.set(sucessed);
		} catch (SQLException e) {
			logger().warn(() -> "execute batch(size: " + l.size() + ") error, operation may not take effect. reason:", e);
		}
	}

	@Override
	protected void doUpsert(Connection conn, String t, String keyField, List<Rmap> l, AtomicLong count) {
		PreparedStatement fullStatement = null;
		ResultSet fullResultSet = null;
		for (int i = 0; i < l.size(); i++) {
			String sql = "SELECT " + keyField + " FROM " + t + " where " + keyField + " = " + l.get(i).get(keyField);
			try {
				fullStatement = conn.prepareStatement(sql);
				fullResultSet = fullStatement.executeQuery();
				if (fullResultSet.next()) {
					doUpdate(conn, keyField, t, l.get(i));
				} else {
					doInsert(conn, keyField, t, l.get(i));
				}
			} catch (SQLException e) {
				logger.error("do upsert fail", e);
			}
		}

	}

	private void doUpdate(Connection conn, String k, String t, Rmap m) {
		String sql;
		StringBuilder sb = new StringBuilder("update ");
		sb.append(t);
		sb.append(" set ");
		for (Map.Entry<String, Object> entry : m.entrySet()) {
			if (!k.equals(entry.getKey())) {
				sb.append(entry.getKey());
				sb.append(" = ");
				sb.append("'");
				sb.append(entry.getValue());
				sb.append("'");
				sb.append(",");
			}
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append(" where ");
		sb.append(k);
		sb.append(" = ");
		sb.append("'");
		sb.append(m.get(k));
		sb.append("'");
		sql = sb.toString();
		try (Statement stmt = conn.createStatement();) {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void doInsert(Connection conn, String k, String t, Rmap m) {
		String sql;
		StringBuilder sb = new StringBuilder("insert into ");
		StringBuilder sbu = new StringBuilder();
		sbu.append("values (");
		sb.append(t);
		sb.append("( ");
		for (Map.Entry<String, Object> entry : m.entrySet()) {
			sb.append(entry.getKey());
			sb.append(" ,");
			sbu.append("'");
			sbu.append(entry.getValue());
			sbu.append("'");
			sbu.append(",");
		}
		sb.deleteCharAt(sb.length() - 1);
		sbu.deleteCharAt(sbu.length() - 1);
		sb.append(")");
		sbu.append(")");
		sql = sb.toString() + sbu.toString();
		try (Statement stmt = conn.createStatement();) {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}