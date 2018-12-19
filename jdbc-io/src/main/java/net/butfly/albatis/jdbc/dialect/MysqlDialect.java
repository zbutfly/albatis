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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

@DialectFor(subSchema = "mysql", jdbcClassname = "com.mysql.cj.jdbc.Driver")
public class MysqlDialect extends Dialect {
	private static final String UPSERT_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s";

	@Override
	protected void doUpsert(Connection conn, String table, String keyField, List<Rmap> records, AtomicLong count){
		doInsertOnUpsert(conn, table, records, count);
	}

	@Override
	protected void doInsertOnUpsert(Connection conn, String table, List<Rmap> records, AtomicLong count) {
		records.forEach(r -> {
			List<String> allFields = new ArrayList<>(r.keySet());
			List<String> nonKeyFields = allFields.stream()/* .filter(f -> !f.equals(keyField)) */.collect(Collectors.toList());

			String fieldnames = allFields.stream().map(this::quota).collect(Collectors.joining(", "));
			String values = allFields.stream().map(f -> "?").collect(Collectors.joining(", "));
			String updates = nonKeyFields.stream().map(f -> quota(f) + " = ?").collect(Collectors.joining(", "));
			String sql = String.format(UPSERT_SQL_TEMPLATE, table, fieldnames, values, updates);

			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				int isize = allFields.size();
				try {
					for (int i = 0; i < allFields.size(); i++) {
						Object value = r.get(allFields.get(i));
						Dialects.setObject(ps, i + 1, value);
					}
					for (int i = 0; i < nonKeyFields.size(); i++) {
						Object value = r.get(nonKeyFields.get(i));
						Dialects.setObject(ps, isize + i + 1, value);
					}
					ps.addBatch();
				} catch (SQLException e) {
					logger().warn(() -> "add `" + records + "` to batch error, ignore this message and continue.", e);
				}
				int[] rs = ps.executeBatch();
				count.addAndGet(Arrays.stream(rs).filter(rr -> rr >= 0).count());
			} catch (SQLException e) {
				logger().warn(() -> "execute batch(size: " + records.size() + ") error, operation may not take effect. reason:", e);
			}
		});
	}

	protected String quota(String f) {
		return '`' == f.charAt(0) && '`' == f.charAt(f.length() - 1) ? f : "`" + f + "`";
	}

	@Override
	public void tableConstruct(Connection conn, String table, TableDesc tableDesc, List<FieldDesc> fields) {
		StringBuilder createSql = new StringBuilder();
		StringBuilder createIndex = new StringBuilder();
		List<String> fieldSql = new ArrayList<>();
		for (FieldDesc field : fields)
			fieldSql.add(buildSqlField(tableDesc, field));
		List<Map<String, Object>> indexes = tableDesc.indexes;
		if (!indexes.isEmpty()) {
			for (int i = 0, len = indexes.size(); i < len; i++) {
				Map<String, Object> indexMap = indexes.get(i);
				String type = (String) indexMap.get("type");
				String alias = (String) indexMap.get("alias");
				List<String> fieldList = JsonUtils.parseFieldsByJson(indexMap.get("field"));
				createIndex.append(type).append(" ").append(alias).append("(")//
						.append(String.join(",", fieldList.toArray(new String[0]))).append(")");
				if (i < len - 1) createIndex.append(",");
			}
		}
		createSql.append("create table ").append(table).append("(").append(String.join(",", fieldSql.toArray(new String[0])));
		if (!indexes.isEmpty()) createSql.append(",").append(createIndex.toString());
		createSql.append(")");
		try (PreparedStatement ps = conn.prepareStatement(createSql.toString())) {
			ps.execute();
			logger().debug("execute ``````" + createSql + "`````` success");
		} catch (SQLException e) {
			throw new RuntimeException("create sql table failure  " + e);
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	protected String buildSqlField(TableDesc tableDesc, FieldDesc field) {
		StringBuilder sb = new StringBuilder();
		switch (field.type.flag) {
		case BOOL:
		case BYTE:
			sb.append(field.name).append(" tinyint(1)");
			break;
		case SHORT:
		case BINARY:
			sb.append(field.name).append(" int(8)");
			break;
		case INT:
			sb.append(field.name).append(" int(16)");
			break;
		case LONG:
			sb.append(field.name).append(" int(64)");
			break;
		case FLOAT:
		case DOUBLE:
			sb.append(field.name).append(" double(16,2)");
			break;
		case STR:
		case CHAR:
		case UNKNOWN:
			sb.append(field.name).append(" varchar(50)");
			break;
		case DATE:
			sb.append(field.name).append(" datetime");
			break;
		default:
			break;
		}
		if (tableDesc.keys.get(0).contains(field.name)) sb.append(" not null primary key");
		return sb.toString();
	}

	@Override
	public boolean tableExisted(Connection conn, String table) {
		DatabaseMetaData dbm = null;
		try {
			dbm = conn.getMetaData();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try (ResultSet rs = dbm.getTables(null, null, table, null)) {
			return rs.next();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
