package net.butfly.albatis.jdbc;

import static net.butfly.albatis.ddl.vals.ValType.Flags.DATE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.INT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.LONG;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STR;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.jdbc.dialect.*;

public class JdbcConnection extends DataConnection<DataSource> {
	private static final Logger logger = Logger.getLogger(JdbcConnection.class);
	final Dialect dialect;

	public JdbcConnection(URISpec uri) throws IOException {
		super(uri, "jdbc");
		dialect = Dialect.of(uri.getScheme());
	}

	@Override
	protected DataSource initialize(URISpec uri) {
		Dialect dialect = Dialect.of(uri.getScheme());
		try {
			return new HikariDataSource(toConfig(dialect, uri));
		} catch (Throwable t) {
			logger().error("jdbc connection fail on [" + uri.toString() + "]");
			throw new RuntimeException(t);
		}
	}

	@Override
	public void construct(String table, FieldDesc... fields) {
		try (Connection conn = client.getConnection()) {
			if (uri.getScheme().startsWith("jdbc:oracle")) {
				StringBuilder sql = new StringBuilder();
				List<String> fieldSql = new ArrayList<>();
				for (FieldDesc f : fields)
					fieldSql.add(buildField(f));
				sql.append("create table ").append(table).append("(").append(String.join(",", fieldSql.toArray(new String[0]))).append(")");
				try (PreparedStatement ps = conn.prepareStatement(sql.toString());) {
					ps.execute();
					logger().info("Table constructed by:\n\t" + sql);
				} catch (SQLException e) {
					logger().error("Table construct failed", e);
				}
			} else throw new UnsupportedOperationException("Jdbc table create not supported for:" + uri.getScheme());
		} catch (SQLException e1) {
		}
	}

	@Override
	public void construct(String table, TableDesc tableDesc, List<FieldDesc> fields) {
		String[] tables = table.split("\\.");
		String dbName, tableName;
		if (tables.length == 1)
			dbName = tableName = tables[0];
		else if ((tables.length == 2)) {
			dbName = tables[0];
			tableName = tables[1];
		} else throw new RuntimeException("Please type in corrent es table format: db.table !");
		try (Connection conn = client.getConnection()) {
			if (uri.getScheme().startsWith("jdbc:mysql"))
				new MysqlDialect().tableConstruct(conn, tableName, tableDesc, fields);
			if (uri.getScheme().startsWith("jdbc:oracle:thin"))
				new OracleDialect().tableConstruct(conn, tableName, tableDesc, fields);
			if (uri.getScheme().startsWith("jdbc:postgresql:libra"))
				new LibraDialect().tableConstruct(conn, tableName, tableDesc, fields);
			if (uri.getScheme().startsWith("jdbc:postgresql") || uri.getScheme().startsWith("jdbc:kingbaseanalyticsdb"))
				new PostgresqlDialect().tableConstruct(conn, tableName, tableDesc, fields);
		} catch (SQLException e) {
			logger().error("construct table failure", e);
		}

	}

	@Override
	public void alterFields(String table, TableDesc tableDesc, List<FieldDesc> fields) {
		try (Connection conn = client.getConnection()) {
			if (uri.getScheme().startsWith("jdbc:postgresql:libra"))
				new LibraDialect().alterColumn(conn, table, tableDesc, fields);
			else
				throw new UnsupportedOperationException("not support this schema operate");
		} catch (SQLException e) {
			logger().error("alert fields failure", e);
		}
	}

	@Override
	public boolean judge(String table) {
		String[] tables = table.split("\\.");
		String dbName, tableName;
		if (tables.length == 1)
			dbName = tableName = tables[0];
		else if ((tables.length == 2)) {
			dbName = tables[0];
			tableName = tables[1];
		} else throw new RuntimeException("Please type in corrent es table format: db.table !");
		try (Connection conn = client.getConnection()) {
			if (uri.getScheme().startsWith("jdbc:mysql") || uri.getScheme().startsWith("jdbc:oracle:thin") || uri.getScheme().startsWith(
					"jdbc:postgresql")) return new MysqlDialect().tableExisted(conn, tableName);
			if (uri.getScheme().startsWith("jdbc:kingbaseanalyticsdb"))
				return new KingbaseDialect().tableExisted(conn, tableName);
		} catch (SQLException e) {
			logger().error("jdbc judge table isExists error", e);
		}
		return false;
	}

	private static HikariConfig toConfig(Dialect dialect, URISpec uriSpec) {
		HikariConfig config = new HikariConfig();
		DialectFor d = dialect.getClass().getAnnotation(DialectFor.class);
		config.setPoolName(d.subSchema() + "-Hikari-Pool");
		if (!"".equals(d.jdbcClassname())) {
			try {
				Class.forName(d.jdbcClassname());
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("JDBC driver class [" + d.jdbcClassname() + "] not found, need driver lib jar file?");
			}
			config.setDriverClassName(d.jdbcClassname());
		}
		String jdbcconn = dialect.jdbcConnStr(uriSpec);
		logger.info("Connect to jdbc with connection string: \n\t" + jdbcconn);
		config.setJdbcUrl(jdbcconn);
		config.setUsername(uriSpec.getParameter("user"));
		config.setPassword(uriSpec.getParameter("password"));
		uriSpec.getParameters().forEach(config::addDataSourceProperty);
		return config;
	}

	@Override
	public void close() {
		DataSource hds = client;
		if (null != hds && hds instanceof AutoCloseable) try {
			((AutoCloseable) hds).close();
		} catch (Exception e) {
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public JdbcInput inputRaw(TableDesc... sql) throws IOException {
		if (sql.length > 1) throw new UnsupportedOperationException("Multiple sql input");
		JdbcInput i;
		try {
			i = new JdbcInput("JdbcInput", this, sql[0].name);
			i.query("select * from " + sql[0].name);
		} catch (SQLException e) {
			throw new IOException(e);
		}
		return i;
	}

	@SuppressWarnings("unchecked")
	@Override
	public JdbcOutput outputRaw(TableDesc... table) throws IOException {
		return new JdbcOutput("JdbcOutput", this);
	}

	public static class Driver implements net.butfly.albatis.Connection.Driver<JdbcConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public JdbcConnection connect(URISpec uriSpec) throws IOException {
			return new JdbcConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("jdbc");
		}
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
