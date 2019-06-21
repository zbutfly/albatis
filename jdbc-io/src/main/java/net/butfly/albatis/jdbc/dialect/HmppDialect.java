package net.butfly.albatis.jdbc.dialect;

import static net.butfly.albatis.ddl.vals.ValType.Flags.BINARY;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BOOL;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BYTE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.CHAR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DATE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DOUBLE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.FLOAT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.INT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.LONG;
import static net.butfly.albatis.ddl.vals.ValType.Flags.SHORT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.UNKNOWN;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.io.Rmap;

@DialectFor(subSchema = "hmppanalyticsdb", jdbcClassname = "com.hmpp.hmppanalyticsdb.Driver")
public class HmppDialect extends Dialect {
	private static final String UPSERT_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s)";
	private static final String QUERY_SQL_TEMPLATE = "SELECT %s FROM %s WHERE %s = ?";
	private static final String UPDTE_SQL_TEMPLATE = "UPDATE %s SET %s WHERE %s = ?";

	@Override
	protected void doInsertOnUpsert(Connection conn, String table, List<Rmap> records, AtomicLong count) {
		records.sort((m1, m2) -> m2.size() - m1.size());
		List<String> allFields = new ArrayList<>(records.get(0).keySet());

		String fieldnames = allFields.stream().collect(Collectors.joining(", "));
		String values = allFields.stream().map(f -> "?").collect(Collectors.joining(", "));
		String sql = String.format(UPSERT_SQL_TEMPLATE, table, fieldnames, values);

		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			records.forEach(m -> {
				try {
					for (int i = 0; i < allFields.size(); i++) {
						Object value = m.get(allFields.get(i));
						Dialects.setObject(ps, i + 1, value);
					}
					ps.addBatch();
				} catch (SQLException e) {
					logger().warn(() -> "add `" + m + "` to batch error, ignore this message and continue.", e);
				}
			});
			int[] rs = ps.executeBatch();
			long sucessed = Arrays.stream(rs).filter(r -> r >= 0).count();
			count.addAndGet(sucessed);
		} catch (SQLException e) {
			logger().warn(
					() -> "execute batch(size: " + records.size() + ") error, operation may not take effect. reason:",
					e);
		}
	}

	@Override
	protected void doUpsert(Connection conn, String table, String keyField, List<Rmap> records, AtomicLong count) {
		records.forEach(r -> {
			String sql = String.format(QUERY_SQL_TEMPLATE, keyField, table, keyField);
			ResultSet rs = null;
			PreparedStatement ps = null;
			try {
				ps = conn.prepareStatement(sql);
				Dialects.setObject(ps, 1, r.get(keyField));
				rs = ps.executeQuery();
				if (rs.next())
					doUpdate(conn, keyField, table, r);
				else
					doInsert(conn, keyField, table, r);
			} catch (SQLException e) {
				logger().error("do upsert fail", e);
			} finally {
				try {
					if (null != rs)
						rs.close();
					if (null != ps)
						ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		});
	}

	private void doUpdate(Connection conn, String k, String t, Rmap m) {
		List<String> allFields = new ArrayList<>(m.keySet());
		List<String> nonKeyFields = allFields.stream().filter(f -> !f.equals(k)).collect(Collectors.toList());
		String updates = nonKeyFields.stream().map(f -> f + " = ?").collect(Collectors.joining(", "));
		String sql = String.format(UPDTE_SQL_TEMPLATE, t, updates, k);
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sql);
			for (int i = 0; i < nonKeyFields.size(); i++) {
				Object value = m.get(nonKeyFields.get(i));
				Dialects.setObject(ps, i + 1, value);
			}
			Dialects.setObject(ps, allFields.size(), m.get(k));
			ps.execute();
		} catch (SQLException e) {
			logger().error("do upsert fail", e);
		} finally {
			try {
				if (null != ps)
					ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private void doInsert(Connection conn, String k, String t, Rmap m) {
		List<Rmap> list = new ArrayList<>();
		list.add(m);
		doInsertOnUpsert(conn, t, list, new AtomicLong());
	}

	@Override
	public boolean tableExisted(Connection conn, String table) {
		String sql = "select * from pg_tables where schemaname = 'public' " + " and tablename = " + "'" + table + "'";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				return true;
			}
		} catch (SQLException e) {
			logger().error("Judging whether table is exists that is failure", e);
		}
		return false;
	}

	@Override
	public String buildCreateTableSql(String table, FieldDesc... fields) {
		StringBuilder sql = new StringBuilder();
		List<String> fieldSql = new ArrayList<>();
		for (FieldDesc f : fields)
			fieldSql.add(buildPostgreField(f));
		sql.append("create table ").append(table).append("(").append(String.join(",", fieldSql.toArray(new String[0])))
				.append(")");
		return sql.toString();
	}

	private String buildPostgreField(FieldDesc field) {
		StringBuilder sb = new StringBuilder();
		switch (field.type.flag) {
		case INT:
		case SHORT:
		case BINARY:
			sb.append(field.name).append(" int4");
			break;
		case LONG:
			sb.append(field.name).append(" int8");
			break;
		case FLOAT:
		case DOUBLE:
			sb.append(field.name).append(" numeric");
			break;
		case BYTE:
		case BOOL:
			sb.append(field.name).append(" bool");
			break;
		case STR:
		case CHAR:
		case UNKNOWN:
			sb.append(field.name).append(" varchar");
			break;
		case DATE:
			sb.append(field.name).append(" date");
			break;
		default:
			break;
		}
//		if (field.rowkey)
//			sb.append(" not null primary key");
		return sb.toString();
	}
}
