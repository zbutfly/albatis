package net.butfly.albatis.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.jdbc.dialect.Dialect;
import net.butfly.albatis.jdbc.dialect.DialectFor;

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
		String sql = dialect.buildCreateTableSql(table, fields);
		logger().info("Table constructed with statment:\n\t" + sql);
		try (Connection conn = client.getConnection(); PreparedStatement ps = conn.prepareStatement(sql.toString());) {
			ps.execute();
		} catch (SQLException e) {
			logger().error("Table construct failed", e);
		}
	}

	@Override
	public void construct(String table, TableDesc tableDesc, List<FieldDesc> fields) {
		String[] tables = table.split("\\.");
		String tableName;
		if (tables.length == 1) tableName = tables[0];
		else if ((tables.length == 2)) tableName = tables[1];
		else throw new RuntimeException("Please type in corrent es table format: db.table !");
		try (Connection conn = client.getConnection()) {
			dialect.tableConstruct(conn, tableName, tableDesc, fields);
		} catch (SQLException e) {
			logger().error("construct table failure", e);
		}

	}

	@Override
	public void alterFields(String table, TableDesc tableDesc, List<FieldDesc> fields) {
		try (Connection conn = client.getConnection()) {
			dialect.alterColumn(conn, table, tableDesc, fields);
		} catch (SQLException e) {
			logger().error("alert fields failure", e);
		}
	}

	@Override
	public List<Map<String, Object>> getResultListByCondition(String table, Map<String, Object> condition) {
		List<Map<String, Object>> results = new ArrayList<>();
		try (Connection conn = client.getConnection()) {
			dialect.getResultListByCondition(conn, table, condition);
		} catch (SQLException e) {
			logger().error("Getting results by condition is failure", e);
		}
		return results;
	}

	@Override
	public void deleteByCondition(String table, Map<String, Object> condition) {
		try (Connection conn = client.getConnection()) {
			dialect.deleteByCondition(conn, table, condition);
		} catch (SQLException e) {
			logger().error("Deleting by condition is failure", e);
		}
	}

	@Override
	public boolean judge(String table) {
		String[] tables = table.split("\\.");
		String tableName;
		if (tables.length == 1) tableName = tables[0];
		else if ((tables.length == 2)) tableName = tables[1];
		else throw new RuntimeException("Please type in corrent es table format: db.table !");
		try (Connection conn = client.getConnection()) {
			dialect.tableExisted(conn, tableName);
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
		try {
			InputStream in = JdbcConnection.class.getClassLoader().getResourceAsStream("ssl.properties");
			if (null != in){
				logger.info("Connect to jdbc with ssl model");
				Properties props = new Properties();
				props.load(in);
				for (String key : props.stringPropertyNames()){
					System.setProperty(key, props.getProperty(key));
				}
			}
		} catch (IOException e) {
			logger.error("load ssl.properties error", e);
		}
		return config;
	}

	@Override
	public void close() {
		DataSource hds = client;
		if (null != hds && hds instanceof AutoCloseable) try {
			((AutoCloseable) hds).close();
		} catch (Exception e) {}
	}

	@SuppressWarnings("unchecked")
	@Override
	public JdbcInput inputRaw(TableDesc... sql) throws IOException {
		if (sql.length > 1) throw new UnsupportedOperationException("Multiple sql input");
		JdbcInput i;
		try {
			i = new JdbcInput("JdbcInput", this, sql[0].name);
			i.query("select * from " + sql[0].name); // XXX: ??why not in constructor?
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
}
