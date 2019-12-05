package net.butfly.albatis.jdbc.dialect;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.zaxxer.hikari.HikariConfig;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.utils.JsonUtils;
import net.butfly.albatis.jdbc.JdbcConnection;

import static net.butfly.albatis.ddl.vals.ValType.Flags.*;

@DialectFor(subSchema = "postgresql", jdbcClassname = "org.postgresql.Driver")
public class PostgresqlDialect extends Dialect {
	private static final Logger logger = Logger.getLogger(PostgresqlDialect.class);
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
                        if (value instanceof CharSequence) value = value.toString().replaceAll("[^\u0020-\u9FA5]", "");
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
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	protected void doUpsert(Connection conn, String table, String keyField, List<Rmap> records, AtomicLong count) {
//		records.sort((m1, m2) -> m2.size() - m1.size());
		Set<String> fieldNameSet = new HashSet<>();
		records.forEach(r -> fieldNameSet.addAll(r.keySet()));
		List<String> allFields = new ArrayList<>(fieldNameSet);
		List<String> nonKeyFields = allFields.stream().filter(f -> !f.equals(keyField)).collect(Collectors.toList()); // update
																														// fields

		String fieldnames = allFields.stream().collect(Collectors.joining("\",\""));
		String values = allFields.stream().map(f -> "?").collect(Collectors.joining(","));
		String updates = nonKeyFields.stream().map(f -> "\"" + f + "\"" + " = EXCLUDED.\"" + f + "\"")
				.collect(Collectors.joining(", "));
		String sql = String.format(UPSERT_SQL_TEMPLATE, table, fieldnames, values, keyField, updates);

		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			records.forEach(m -> {
				try {
					for (int i = 0; i < allFields.size(); i++) {
						Object value = m.get(allFields.get(i));
//                        if (value instanceof CharSequence) value = value.toString().replaceAll("[^\u0020-\u9FA5]", "");
                        if (value instanceof CharSequence) value = value.toString().replaceAll("[\u0000]", "");
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
		} catch (Exception e) {
			logger().warn(
					() -> "execute batch(size: " + records.size() + ") error, operation may not take effect. reason:",
					e);
		} finally {
			try {
				if (null != conn)
					conn.close();
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
			sb.append("create table ").append("\"").append(table).append("\"").append("(")
					.append(String.join(",", fieldSql.toArray(new String[0]))).append(")");
			statement.addBatch(sb.toString());
			if (!indexes.isEmpty()) {
				for (Map<String, Object> index : indexes) {
					StringBuilder createIndex = new StringBuilder();
					String type = (String) index.get("type");
					String alias = (String) index.get("alias");
					List<String> fieldList = JsonUtils.parseFieldsByJson(index.get("field"));
					createIndex.append("create ").append(type).append(" ").append(alias).append(" on ").append("\"")
							.append(table).append("\"").append("(").append("\"")
							.append(String.join("\",\"", fieldList.toArray(new String[0]))).append("\"").append(")");
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
			logger().error("Getting metadata is failure", e);
		}
		try (ResultSet rs = dbm.getTables(null, null, table, null)) {
			return rs.next();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
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
		case TIMESTAMP:
			sb.append(field.name).append(" timestamp");
			break;
		case GEO_PG_GEOMETRY:
			sb.append(field.name).append(" geometry");
			break;
		default:
			break;
		}
		if (field.rowkey)
			sb.append(" not null primary key");
		return sb.toString();
	}

	@Override
	public String jdbcConnStr(URISpec uriSpec) {
		StringBuilder url = new StringBuilder();
		if (uriSpec.toString().contains("?")) {
			url.append(uriSpec.getSchema()).append("://").append(uriSpec.getHost()).append("/")
					.append(uriSpec.getFile()).append("?").append(uriSpec.toString().split("\\?")[1]);
			URISpec uri = new URISpec(url.toString());
			return uri.toString();
		} else {
			url.append(uriSpec.getSchema()).append("://").append(uriSpec.getHost()).append("/")
					.append(uriSpec.getFile());
			URISpec uri = new URISpec(url.toString());
			return uri.toString();
		}
	}

	@Override
	public HikariConfig toConfig(Dialect dialect, URISpec uriSpec) {
		HikariConfig config = new HikariConfig();
		String maximumPoolSize = Configs.gets("albatis.jdbc.maximumpoolsize");
		String minimumIdle = Configs.gets("albatis.jdbc.minimumIdle");
		String idleTimeOut = Configs.gets("albatis.jdbc.idleTimeout");
		if (null != maximumPoolSize && !"".equals(maximumPoolSize))
			config.setMaximumPoolSize(Integer.parseInt(maximumPoolSize));
		if (null != minimumIdle && !"".equals(minimumIdle))
			config.setMinimumIdle(Integer.parseInt(minimumIdle));
		if (null != idleTimeOut && "".equals(idleTimeOut))
			config.setIdleTimeout(Long.parseLong(idleTimeOut));
		DialectFor d = dialect.getClass().getAnnotation(DialectFor.class);
		config.setPoolName(d.subSchema() + "-Hikari-Pool");
		if (!"".equals(d.jdbcClassname())) {
			try {
				Class.forName(d.jdbcClassname());
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(
						"JDBC driver class [" + d.jdbcClassname() + "] not found, need driver lib jar file?");
			}
			config.setDriverClassName(d.jdbcClassname());
		}
		String jdbcconn = dialect.jdbcConnStr(uriSpec);
		logger.info("Connect to jdbc with connection string: \n\t" + jdbcconn);
		config.setJdbcUrl(jdbcconn.split("\\?")[0]);
		if (null == uriSpec.getParameter("user")) {
			config.setUsername(uriSpec.getAuthority().split(":")[0].toString());
			config.setPassword(uriSpec.getAuthority().split(":")[1].substring(0,
					uriSpec.getAuthority().split(":")[1].lastIndexOf("@")));
		} else {
			config.setUsername(uriSpec.getParameter("username"));
			config.setPassword(uriSpec.getParameter("password"));
		}
		uriSpec.getParameters().forEach(config::addDataSourceProperty);
		try {
			InputStream in = JdbcConnection.class.getClassLoader().getResourceAsStream("ssl.properties");
			if (null != in) {
				logger.info("Connect to jdbc with ssl model");
				Properties props = new Properties();
				props.load(in);
				for (String key : props.stringPropertyNames()) {
					System.setProperty(key, props.getProperty(key));
				}
			}
		} catch (IOException e) {
			logger.error("load ssl.properties error", e);
		}
		return config;
	}
}
