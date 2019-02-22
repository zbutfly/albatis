package net.butfly.albatis.jdbc.dialect;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.utils.JsonUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static net.butfly.albatis.ddl.vals.ValType.Flags.*;

@DialectFor(subSchema = "postgresql:libra", jdbcClassname = "org.postgresql.Driver")
public class LibraDialect extends Dialect {

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

}
