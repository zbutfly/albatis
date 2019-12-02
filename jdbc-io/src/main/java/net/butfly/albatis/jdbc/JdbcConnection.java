package net.butfly.albatis.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.PrivilegedAction;
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
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.jdbc.dialect.Dialect;
import net.butfly.albatis.jdbc.dialect.DialectFor;
import org.apache.hadoop.security.UserGroupInformation;

public class JdbcConnection extends DataConnection<DataSource> {
	private static final Logger logger = Logger.getLogger(JdbcConnection.class);
	public static String KERBEROS_CONF_PATH = null;
	final Dialect dialect;

	public JdbcConnection(URISpec uri) throws IOException {
		super(uri, "jdbc");
		dialect = Dialect.of(uri.getSchema());
	}

	@Override
	protected DataSource initialize(URISpec uri) {
		Dialect dialect = Dialect.of(uri.getSchema());
		if(null != uri.getParameter("kerberos")){
			KERBEROS_CONF_PATH = uri.getParameter("kerberos");
		}else {
			KERBEROS_CONF_PATH = null;
		}
		if(null!=KERBEROS_CONF_PATH){
			new Kerberos(KERBEROS_CONF_PATH);
			UserGroupInformation loginUser = null;
			try {
				loginUser = UserGroupInformation.getLoginUser();
			} catch (IOException e) {
				e.printStackTrace();
			}
			HikariDataSource h = loginUser.doAs((PrivilegedAction<HikariDataSource>) () -> {
				HikariDataSource h2 = new HikariDataSource(dialect.toConfig(dialect, uri));
				return h2;
			});
			return h;
		}
		try {
			HikariDataSource h = new HikariDataSource(dialect.toConfig(dialect, uri));
			return h;
		} catch (Throwable t) {
			logger().error("jdbc connection fail on [" + uri.toString() + "]");
			throw new RuntimeException(t);
		}
	}

	@Override
	public void construct(Qualifier qualifier, FieldDesc... fields) {
		String sql = dialect.buildCreateTableSql(qualifier.name, fields);
		logger().info("Table constructed with statment:\n\t" + sql);
		try (Connection conn = client.getConnection(); PreparedStatement ps = conn.prepareStatement(sql.toString());) {
			ps.execute();
		} catch (SQLException e) {
			logger().error("Table construct failed", e);
		}
	}

	@Override
	public void construct(String table, TableDesc tableDesc, List<FieldDesc> fields) {
		String tableName;
		Connection conn = null;
		String[] tables = table.split("\\.");
		if (tables.length == 1)
			tableName = tables[0];
		else if (tables.length == 2)
			tableName = tables[1];
		else
			throw new RuntimeException("Please type in correct jdbc table format: db.table !");
		try {
			conn = client.getConnection();
			dialect.tableConstruct(conn, tableName, tableDesc, fields);
		} catch (SQLException e) {
			logger().error("construct table failure", e);
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
			results = dialect.getResultListByCondition(conn, table, condition);
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
		String tableName;
		Connection conn = null;
		String[] tables = table.split("\\.");
		if (tables.length == 1)
			tableName = tables[0];
		else if (tables.length == 2)
			tableName = tables[1];
		else
			throw new RuntimeException("Please type in correct jdbc table format: db.table !");
		try {
			conn = client.getConnection();
			return dialect.tableExisted(conn, tableName);
		} catch (SQLException e) {
			logger().error("jdbc judge table isExists error", e);
		} finally {
			try {
				if (null != conn)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return false;
	}

	@Override
	public void close() {
		DataSource hds = client;
		if (null != hds && hds instanceof AutoCloseable)
			try {
				((AutoCloseable) hds).close();
			} catch (Exception e) {
			}
	}

	@SuppressWarnings("unchecked")
	@Override
	public JdbcInput inputRaw(TableDesc... sql) throws IOException {
		if (sql.length > 1)
			throw new UnsupportedOperationException("Multiple sql input");
		JdbcInput i;
		try {
			i = new JdbcInput("JdbcInput", this, sql[0].qualifier.name);
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
