package net.butfly.albatis.jdbc;

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

import net.butfly.albacore.paral.Exeter;
import net.butfly.albatis.io.Rmap;

/**
 * @author zhuqh
 * @create 2018-09-29 15:20
 */
public class KingBaseUpserter extends Upserter {
	private static final String psql = "insert into %s (%s) values(%s)";

	public KingBaseUpserter(Type type) {
		super(type);
	}

	@Override
	long upsert(Map<String, List<Rmap>> mml, Connection conn) {
		AtomicLong count = new AtomicLong();
		Exeter.of().join(entry -> {
			mml.forEach((t, l) -> {
				if (l.isEmpty()) return;
				String keyField = determineKeyField(entry.getValue());
				// String keyField = getKeyField(l);

				List<Rmap> ml = l.stream().sorted((m1, m2) -> m2.size() - m1.size()).collect(Collectors.toList());
				List<String> fl = new ArrayList<>(ml.get(0).keySet());
				String fields = fl.stream().collect(Collectors.joining(", "));
				String values = fl.stream().map(f -> "?").collect(Collectors.joining(", "));
				List<String> ufields = fl.stream().filter(f -> !f.equals(keyField)).collect(Collectors.toList()); // update fields
				String updates = ufields.stream().map(f -> f + " = EXCLUDED." + f).collect(Collectors.joining(", "));
				String sql = String.format(psql, t, fields, values, keyField, updates);

				if (null == keyField) {
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

				} else {
					select(conn, keyField, t, l);
				}
			});
		}, mml.entrySet());
		return count.get();
	}

	protected static String getKeyField(List<Rmap> list) {
		if (null == list || list.isEmpty()) return null;
		Rmap msg = list.get(0);
		Object key = msg.key();
		if (null == key) return null;
		return key.toString();
	}

	protected static void select(Connection conn, String k, String t, List<Rmap> list) {

		PreparedStatement fullStatement = null;
		ResultSet fullResultSet = null;
		for (int i = 0; i < list.size(); i++) {
			String sql = "SELECT " + k + " FROM " + t + " where " + k + " = " + list.get(i).get(k);
			try {
				fullStatement = conn.prepareStatement(sql);
				fullResultSet = fullStatement.executeQuery();
				if (fullResultSet.next()) {
					update(conn, k, t, list.get(i));
				} else {
					insert(conn, k, t, list.get(i));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	protected static void update(Connection conn, String k, String t, Rmap m) {
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

	protected static void insert(Connection conn, String k, String t, Rmap m) {
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