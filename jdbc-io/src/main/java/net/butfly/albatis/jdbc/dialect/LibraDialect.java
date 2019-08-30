package net.butfly.albatis.jdbc.dialect;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.utils.JsonUtils;

import static net.butfly.albatis.ddl.vals.ValType.Flags.*;

@DialectFor(subSchema = "postgresql:libra", jdbcClassname = "org.postgresql.Driver")
public class LibraDialect extends Dialect {
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
			logger().warn(() -> "execute batch(size: " + records.size() + ") error, operation may not take effect. reason:", e);
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
				if (rs.next()) doUpdate(conn, keyField, table, r);
				else doInsert(conn, keyField, table, r);
			} catch (SQLException e) {
				logger().error("do upsert fail", e);
			} finally {
				try {
					if (null != rs) rs.close();
					if (null != ps) ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		});
	}

	private void doUpdate(Connection conn, String k, String t, Rmap m) {
		List<String> allFields = new ArrayList<>(m.keySet());
		List<String> nonKeyFields = allFields.stream() .filter(f -> !f.equals(k)) .collect(Collectors.toList());
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
				if (null != ps) ps.close();
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
	public String jdbcConnStr(URISpec uri) {
		return "jdbc:postgresql://" + uri.toString().substring(uri.toString().indexOf("//") + 2);
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
			case GEO_PG_GEOMETRY:
				sb.append(field.name).append(" geometry");
			default:
				break;
		}
		if (field.rowkey)
			sb.append(" not null primary key");
		return sb.toString();
	}

	@Override
	public void tableConstruct(Connection conn, String table, TableDesc tableDesc, List<FieldDesc> fields) {
		StringBuilder sb = new StringBuilder();
		List<String> fieldSql = new ArrayList<>();
		for (FieldDesc field : fields)
			fieldSql.add(buildSqlField(tableDesc, field));
		try (Statement statement = conn.createStatement()) {
			List<Map<String, Object>> indexes = tableDesc.indexes;
			sb.append("create table ").append("\"").append(table).append("\"").append("(").append(String.join(",", fieldSql.toArray(
					new String[0]))).append(")");
			// support libra distributed by column
			// Object distributeColumns = tableDesc.construct.get("distribute_columns");
			// if (null == distributeColumns || "".equals(distributeColumns))
			// sb.append("with (oids=false) ").append("distribute by hash ( ").append(String.join(",", tableDesc.keys.get(0).toArray(new
			// String[0]))).append(" )");
			// else
			// sb.append("with (oids=false) ").append("distribute by hash ( ").append(distributeColumns.toString()).append(" )");
			statement.addBatch(sb.toString());
			if (!indexes.isEmpty()) {
				for (Map<String, Object> index : indexes) {
					StringBuilder createIndex = new StringBuilder();
					String type = (String) index.get("type");
					String alias = (String) index.get("alias");
					List<String> fieldList = JsonUtils.parseFieldsByJson(index.get("field"));
					createIndex.append("create ").append(type).append(" ").append(alias).append(" on ").append("\"").append(table).append(
							"\"").append("(").append("\"").append(String.join("\",\"", fieldList.toArray(new String[0]))).append("\"")
							.append(")");
					statement.addBatch(createIndex.toString());
				}
			}
			statement.executeBatch();
			logger().debug("execute ``````" + sb + "`````` success");
		} catch (SQLException e) {
			throw new RuntimeException("create postgre table failure " + e);
		}
	}

	@Override
	protected String buildSqlField(TableDesc tableDesc, FieldDesc field) {
		StringBuilder sb = new StringBuilder();
		switch (field.type.flag) {
			case INT:
			case SHORT:
			case BINARY:
				sb.append("\"").append(field.name).append("\"").append(" int4");
				break;
			case LONG:
				sb.append("\"").append(field.name).append("\"").append(" int8");
				break;
			case FLOAT:
			case DOUBLE:
				sb.append("\"").append(field.name).append("\"").append(" numeric(10)");
				break;
			case BYTE:
			case BOOL:
				sb.append("\'").append(field.name).append("\'").append(" bool(1)");
				break;
			case STR:
			case CHAR:
			case UNKNOWN:
				sb.append("\"").append(field.name).append("\"").append(" varchar(256)");
				break;
			case DATE:
				sb.append("\"").append(field.name).append("\"").append(" date");
				break;
			default:
				break;
		}
		if (tableDesc.keys.get(0).contains(field.name)) sb.append(" not null primary key");
		return sb.toString();
	}

	public List<String> getFieldNameList(Connection conn, String table) {
		List<String> columnNames = new ArrayList<>();
		String sql = "select * from " + table;
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			// 结果集元数据
			ResultSetMetaData rsmd = ps.getMetaData();
			// 表列数
			int size = rsmd.getColumnCount();
			for (int i = 0; i < size; i++) {
				columnNames.add(rsmd.getColumnName(i + 1));
			}
		} catch (SQLException e) {
			logger().error("Getting MPP libra column name list is failure ", e);
		}
		return columnNames;
	}

	@Override
	public void alterColumn(Connection conn, String table, TableDesc tableDesc, List<FieldDesc> fields) {
		List<String> fieldNameList = getFieldNameList(conn, table);
		StringBuilder sb = new StringBuilder();
		List<String> fieldSql = new ArrayList<>();
		for (FieldDesc field : fields) {
			if (!fieldNameList.contains(field.name)) fieldSql.add(buildSqlField(tableDesc, field));
		}
		sb.append("alter table ").append("\"").append(table).append("\"");
		if (fieldSql.size() == 0) return;
		for (int i = 0, len = fieldSql.size(); i < len; i++) {
			sb.append(" add column ").append(fieldSql.get(i));
			if (i < len - 1) sb.append(",");
		}
		try (PreparedStatement ps = conn.prepareStatement(sb.toString())) {
			ps.execute();
			logger().debug("execute ``````" + sb.toString() + "`````` success");
		} catch (SQLException e) {
			logger().error("alert column failure", e);
		}
	}

	@Override
	public List<Map<String, Object>> getResultListByCondition(Connection conn, String table, Map<String, Object> condition) {
		List<Map<String, Object>> resultList = new ArrayList<>();
		StringBuilder parameter = new StringBuilder();
		String sql = "select * from " + table;
		if (condition.size() != 0) {
			for (String key : condition.keySet()) {
				parameter.append(key).append("='").append(condition.get(key)).append("'and ");
			}
			String param = parameter.substring(0, parameter.length() - 4);
			sql = "select * from " + table + " where " + param;
		}
		Statement stmt = null;
		ResultSet rs = null;
		try {
			// 生成预处理语句。
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			ResultSetMetaData rsmd = rs.getMetaData();
			while (rs.next()) {
				// n = rs.getInt(1);
				Map<String, Object> m = new HashMap<>();
				for (int i = 0; i < rsmd.getColumnCount(); i++) {
					String colName = rsmd.getColumnName(i + 1);
					Object colValue = rs.getObject(colName);
					if (colValue == null) {
						colValue = "";
					}
					m.put(colName, colValue);
				}
				resultList.add(m);
			}
			return resultList;
		} catch (SQLException e) {
			logger().error("Get results are failure", e);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					logger().error("Close statement failure", e);
				}
			}
		}
		return null;
	}

	@Override
	public void deleteByCondition(Connection conn, String table, Map<String, Object> condition) {
		StringBuilder parameter = new StringBuilder();
		String sql = "select * from " + table;
		if (condition.size() != 0) {
			for (String key : condition.keySet()) {
				parameter.append(key).append("='").append(condition.get(key)).append("'and ");
			}
			String param = parameter.substring(0, parameter.length() - 4);
			sql = "delete from " + table + " where " + param;
		}
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.execute();
		} catch (SQLException e) {
			logger().info("Delete key information quantity error", e);
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					logger().error("Close statement failure", e);
				}
			}
		}
	}

	@Override
	public boolean tableExisted(Connection conn, String table) {
		DatabaseMetaData dbm = null;
		try {
			dbm = conn.getMetaData();
		} catch (SQLException e) {
			logger().error("Getting metadata is failure", e);
		}
		try (ResultSet rs = dbm.getTables(null, null, table, null)) {
			return rs.next();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public void deleteTable(Connection conn, String table) {
		String sql = "delete from " + table;
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.execute();
			logger().debug("execute ``````" + sql + "`````` success");
		} catch (SQLException e) {
			logger().error("delete table failure", e);
		}
	}

}
