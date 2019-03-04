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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.utils.JsonUtils;

@DialectFor(subSchema = "oracle:thin", jdbcClassname = "oracle.jdbc.OracleDriver")
public class OracleDialect extends Dialect {
	private static final String UPSERT_SQL_TEMPLATE = "MERGE INTO %s USING DUAL ON (%s = ?) WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s) WHEN MATCHED THEN UPDATE SET %s";
	private static final String INSERT_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s)";

	@Override
	protected void doInsertOnUpsert(Connection conn, String table, List<Rmap> records, AtomicLong count) {
		records.sort((m1, m2) -> m2.size() - m1.size());
		List<String> allFields = new ArrayList<>(records.get(0).keySet());

		String fieldnames = allFields.stream().collect(Collectors.joining(", "));
		String values = allFields.stream().map(f -> "?").collect(Collectors.joining(", "));
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
		}
	}

	@Override
	protected void doUpsert(Connection conn, String table, String keyField, List<Rmap> records, AtomicLong count) {
		records.sort((m1, m2) -> m2.size() - m1.size());
		List<String> allFields = new ArrayList<>(records.get(0).keySet());
		List<String> nonKeyFields = allFields.stream().filter(f -> !f.equals(keyField)).collect(Collectors.toList());

		String fieldnames = allFields.stream().collect(Collectors.joining(", "));
		String values = allFields.stream().map(f -> "?").collect(Collectors.joining(", "));
		String updates = nonKeyFields.stream().map(f -> f + " = ?").collect(Collectors.joining(", "));
		String sql = String.format(UPSERT_SQL_TEMPLATE, table, keyField, fieldnames, values, updates);

		logger().debug("ORACLE upsert SQL: " + sql);
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			records.forEach(m -> {
				int offset = 0;
				try {
					Dialects.setObject(ps, offset + 1, m.key());
					offset++;
					for (int i = 0; i < allFields.size(); i++) { // set insert fields value
						Object value = m.get(allFields.get(i));
						Dialects.setObject(ps, offset + i + 1, value);
					}
					offset += allFields.size();
					for (int i = 0; i < nonKeyFields.size(); i++) { // set update fields value
						Object value = m.get(nonKeyFields.get(i));
						Dialects.setObject(ps, offset + i + 1, value);
					}
					offset += nonKeyFields.size();
					ps.addBatch();
				} catch (SQLException e) {
					logger().warn(() -> "add `" + m + "` to batch error, ignore this message and continue." + e.getSQLState());
				}
			});
			int[] rs = ps.executeBatch();
			long sucessed = Arrays.stream(rs).filter(r -> r >= 0).count();
			count.addAndGet(sucessed);
		} catch (SQLException e) {
			logger().warn(() -> "execute batch(size: " + records.size() + ") error, operation may not take effect. reason:", e);
		}
	}

	// : means sid(!sid), / means sid
	@Override
	public String jdbcConnStr(URISpec uri) {
		String db = uri.getPath(1);
		if ("".equals(db)) db = uri.getFile();
		return uri.getScheme() + ":@" + uri.getHost() + (db.charAt(0) == '!' ? (":" + db.substring(1)) : ("/" + db));
	}

	@Override
	public void tableConstruct(Connection conn, String table, TableDesc tableDesc, List<FieldDesc> fields) {
		StringBuilder sb = new StringBuilder();
		List<String> fieldSql = new ArrayList<>();
		for (FieldDesc field : fields)
			fieldSql.add(buildSqlField(tableDesc, field));
		List<Map<String, Object>> indexes = tableDesc.indexes;
		sb.append("create table ").append(table).append("(").append(String.join(",", fieldSql.toArray(new String[0]))).append(")");
		try (Statement statement = conn.createStatement()) {
			statement.addBatch(sb.toString());
			if (!indexes.isEmpty()) {
				for (Map<String, Object> index : indexes) {
					StringBuilder createIndex = new StringBuilder();
					String type = (String) index.get("type");
					String alias = (String) index.get("alias");
					List<String> fieldList = JsonUtils.parseFieldsByJson(index.get("field"));
					createIndex.append("create ").append(type).append(" ").append(alias).append(" on ").append(table).append("(").append(
							String.join(",", fieldList.toArray(new String[0]))).append(")");
					statement.addBatch(createIndex.toString());
				}
			}
			statement.executeBatch();
			logger().debug("execute ``````" + sb + "`````` success");
		} catch (SQLException e) {
			throw new RuntimeException("create oracle table failure " + e);
		}
	}

	@Override
	protected String buildSqlField(TableDesc tableDesc, FieldDesc field) {
		StringBuilder sb = new StringBuilder();
		switch (field.type.flag) {
		case BOOL:
		case BYTE:
		case SHORT:
		case BINARY:
			sb.append(field.name).append(" number(16)");
			break;
		case INT:
			sb.append(field.name).append(" number(32)");
			break;
		case LONG:
			sb.append(field.name).append(" number(64)");
			break;
		case FLOAT:
		case DOUBLE:
			sb.append(field.name).append(" number(32,2)");
			break;
		case STR:
		case CHAR:
		case UNKNOWN:
			sb.append(field.name).append(" varchar2(100)");
			break;
		case DATE:
			sb.append(field.name).append(" date");
			break;
		default:
			break;
		}
		if (tableDesc.keys.get(0).contains(field.name)) sb.append(" not null primary key");
		return sb.toString();
	}

	@Override
	public String buildCreateTableSql(String table, FieldDesc... fields) {
		StringBuilder sql = new StringBuilder();
		List<String> fieldSql = new ArrayList<>();
		for (FieldDesc f : fields)
			fieldSql.add(buildField(f));
		sql.append("create table ").append(table).append("(").append(String.join(",", fieldSql.toArray(new String[0]))).append(")");
		// TODO Auto-generated method stub
		return null;
	}

	private static String buildField(FieldDesc field) {
		StringBuilder sb = new StringBuilder();
		switch (field.type.flag) {
		case INT:
			sb.append(field.name).append(" number(32, 0)");
			break;
		case LONG:
			sb.append(field.name).append(" number(64, 0)");
			break;
		case STR:
			sb.append(field.name).append(" varchar2(100)");
			break;
		case DATE:
			sb.append(field.name).append(" date");
			break;
		default:
			break;
		}
		if (field.rowkey) sb.append(" not null primary key");
		return sb.toString();
	}
}
