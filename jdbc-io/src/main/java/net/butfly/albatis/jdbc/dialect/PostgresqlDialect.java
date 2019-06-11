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

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.utils.JsonUtils;

@DialectFor(subSchema = "postgresql", jdbcClassname = "org.postgresql.Driver")
public class PostgresqlDialect extends Dialect {
	private static final String UPSERT_SQL_TEMPLATE = "INSERT INTO \"%s\" (\"%s\") VALUES (%s) ON CONFLICT(\"%s\") DO UPDATE SET %s";
	private static final String INSERT_SQL_TEMPLATE = "INSERT INTO \"%s\" (\"%s\") VALUES (%s)";

	@Override
	protected void doInsertOnUpsert(Connection conn, String table, List<Rmap> records, AtomicLong count) {
		records.sort((m1, m2) -> m2.size() - m1.size());
		List<String> allFields = new ArrayList<>(records.get(0).keySet());
		String fieldnames = allFields.stream().collect(Collectors.joining("\",\""));
		String values = allFields.stream().map(f -> "?").collect(Collectors.joining(","));
		String sql = String.format(INSERT_SQL_TEMPLATE, table, fieldnames, values);
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
		}finally {
			try {
				if(conn != null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	protected void doUpsert(Connection conn, String table, String keyField, List<Rmap> records, AtomicLong count) {
		records.sort((m1, m2) -> m2.size() - m1.size());
		List<String> allFields = new ArrayList<>(records.get(0).keySet());
		List<String> nonKeyFields = allFields.stream().filter(f -> !f.equals(keyField)).collect(Collectors.toList()); // update fields

		String fieldnames = allFields.stream().collect(Collectors.joining("\",\""));
		String values = allFields.stream().map(f -> "?").collect(Collectors.joining(","));
		String updates = nonKeyFields.stream().map(f -> "\""+ f +"\"" + " = EXCLUDED.\"" + f +"\"").collect(Collectors.joining(", "));
		String sql = String.format(UPSERT_SQL_TEMPLATE, table, fieldnames, values, keyField, updates);

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
		}finally {
			try {
				if(null != conn) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
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
	public boolean tableExisted(Connection conn, String table) {
		DatabaseMetaData dbm = null;
		try {
			dbm = conn.getMetaData();
		} catch (SQLException e) {
			logger().error("Getting metadata is failure",e);
		}
		try (ResultSet rs = dbm.getTables(null, null, table, null)) {
			return rs.next();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}finally {
			if(conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
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
}
