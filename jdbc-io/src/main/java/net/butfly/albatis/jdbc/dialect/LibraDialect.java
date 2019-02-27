package net.butfly.albatis.jdbc.dialect;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.utils.JsonUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static net.butfly.albatis.ddl.vals.ValType.Flags.*;

@DialectFor(subSchema = "postgresql:libra", jdbcClassname = "org.postgresql.Driver")
public class LibraDialect extends Dialect {

	private static final String UPSERT_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s)";

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
			String sql = "SELECT " + keyField + " FROM " + table + " where " + keyField + " = " + r.get(keyField);
			try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery();) {
				if (rs.next()) doUpdate(conn, keyField, table, r);
				else doInsert(conn, keyField, table, r);
			} catch (SQLException e) {
				logger().error("do upsert fail", e);
			}
		});
	}

	private void doUpdate(Connection conn, String k, String t, Rmap m) {
		StringBuilder sb = new StringBuilder("update ").append(t).append(" set ");
		for (Map.Entry<String, Object> entry : m.entrySet())
			if (!k.equals(entry.getKey())) //
				sb.append(entry.getKey()).append(" = ").append("'").append(entry.getValue()).append("'").append(",");
		sb.deleteCharAt(sb.length() - 1).append(" where ").append(k).append(" = ").append("'").append(m.get(k)).append("'");
		try (Statement ps = conn.createStatement();) {
			ps.executeUpdate(sb.toString());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void doInsert(Connection conn, String k, String t, Rmap m) {
		StringBuilder sbu = new StringBuilder("values (");
		StringBuilder sb = new StringBuilder("insert into ").append(t).append("( ");
		for (Map.Entry<String, Object> entry : m.entrySet()) {
			sb.append(entry.getKey()).append(" ,");
			sbu.append("'").append(entry.getValue()).append("'").append(",");
		}
		try (Statement ps = conn.createStatement();) {
			ps.executeUpdate(sb.deleteCharAt(sb.length() - 1).append(")").toString() //
					+ sbu.deleteCharAt(sbu.length() - 1).append(")").toString());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String jdbcConnStr(URISpec uri) {
		return "jdbc:postgresql://" + uri.toString().substring(uri.toString().indexOf("//") + 2);
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
			Object distributeColumns = tableDesc.construct.get("distribute_columns");
			if (null == distributeColumns || "".equals(distributeColumns))
				sb.append("with (oids=false) ").append("distribute by hash ( ").append(String.join(",", tableDesc.keys.get(0).toArray(new String[0]))).append(" )");
			else
				sb.append("with (oids=false) ").append("distribute by hash ( ").append(distributeColumns.toString()).append(" )");
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

	@Override
	public List<String> getFieldNameList(Connection conn, String table) {
		List<String> columnNames = new ArrayList<>();
		String sql = "select * from " + table;
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			//结果集元数据
			ResultSetMetaData rsmd = ps.getMetaData();
			//表列数
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
			if (!fieldNameList.contains(field.name))
				fieldSql.add(buildSqlField(tableDesc, field));
		}
		sb.append("alter table ").append("\"").append(table).append("\"");
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
