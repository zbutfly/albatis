package net.butfly.albatis.jdbc.dialect;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.zaxxer.hikari.HikariConfig;

import net.butfly.albacore.exception.NotImplementedException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.jdbc.JdbcConnection;

@DialectFor
public class Dialect implements Loggable {
	private static final Logger logger = Logger.getLogger(Dialect.class);

	public static Dialect of(String schema) {
		String s = schema.substring(schema.indexOf(':') + 1);
		Set<Class<? extends Dialect>> classes = Reflections.getSubClasses(Dialect.class,
				"net.butfly.albatis.jdbc.dialect");
		int lastpos;
		do {
			for (Class<? extends Dialect> c : classes)
				if (c.getAnnotation(DialectFor.class).subSchema().equals(s)) //
					return Reflections.construct(c);
		} while ((lastpos = s.lastIndexOf(':')) >= 0 && !(s = s.substring(0, lastpos)).isEmpty());
		return new Dialect();
	}

	public String jdbcConnStr(URISpec uriSpec) {
		return uriSpec.toString();
	}

	public long upsert(Map<String, List<Rmap>> allRecords, Connection conn) {
		AtomicLong count = new AtomicLong();
		Exeter.of().join(entry -> {
			String table = entry.getKey();
			List<Rmap> records = entry.getValue();
			if (records.isEmpty())
				return;
			String keyField = null != records.get(0).keyField() ? records.get(0).keyField()
					: Dialects.determineKeyField(records);
			if (null != keyField)
				doUpsert(conn, table, keyField, records, count);
			else
				doInsertOnUpsert(conn, table, records, count);

		}, allRecords.entrySet());
		return count.get();
	}

	protected void doInsertOnUpsert(Connection conn, String t, List<Rmap> l, AtomicLong count) {
		throw new NotImplementedException();
	}

	protected void doUpsert(Connection conn, String t, String keyField, List<Rmap> l, AtomicLong count) {
		throw new NotImplementedException();
	}

	public void tableConstruct(Connection conn, String table, TableDesc tableDesc, List<FieldDesc> fields) {
		throw new NotImplementedException();
	}

	protected String buildSqlField(TableDesc tableDesc, FieldDesc field) {
		throw new NotImplementedException();
	}

	public boolean tableExisted(Connection conn, String table) {
		throw new NotImplementedException();
	}

	public void alterColumn(Connection conn, String table, TableDesc tableDesc, List<FieldDesc> fields) {
		throw new NotImplementedException();
	}

	public List<Map<String, Object>> getResultListByCondition(Connection conn, String table,
			Map<String, Object> condition) {
		throw new NotImplementedException();
	}

	public void deleteByCondition(Connection conn, String table, Map<String, Object> condition) {
		throw new NotImplementedException();
	}

	public String buildCreateTableSql(String table, FieldDesc... fields) {
		throw new NotImplementedException();
	}

	public HikariConfig toConfig(Dialect dialect, URISpec uriSpec) {
		HikariConfig config = new HikariConfig();
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
		config.setUsername(uriSpec.getParameter("username"));
		config.setPassword(uriSpec.getParameter("password"));
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
