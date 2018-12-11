package net.butfly.albatis.jdbc.dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import net.butfly.albacore.exception.NotImplementedException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.jdbc.Type;

public class Dialect implements Loggable {
	final protected Logger logger = Logger.getLogger(this.getClass());
	public final Type type;

	public Dialect(Type type) {
		this.type = type;
	}

	public String jdbcConnStr(URISpec uriSpec) {
		return uriSpec.toString();
	}

	public long upsert(Map<String, List<Rmap>> mml, Connection conn) {
		AtomicLong count = new AtomicLong();
		Exeter.of().join(entry -> {
			String t = entry.getKey();
			List<Rmap> l = entry.getValue();
			if (l.isEmpty()) return;
			String keyField = determineKeyField(l);
			if (null != keyField) doUpsert(conn, t, keyField, l, count);
			else doInsertOnUpsert(conn, t, l, count);
		}, mml.entrySet());
		return count.get();
	}

	protected void doInsertOnUpsert(Connection conn, String t, List<Rmap> l, AtomicLong count) {
		throw new NotImplementedException();
	}

	protected void doUpsert(Connection conn, String t, String keyField, List<Rmap> l, AtomicLong count) {
		throw new NotImplementedException();
	}

	protected void setObject(PreparedStatement ps, int index, Object value) throws SQLException {
		if (value instanceof java.util.Date) {
			java.util.Date jud = (java.util.Date) value;
			// java.sql.Date jsd = new java.sql.Date(jud.getTime()); // 会丢掉时分秒，因java.sql.Date 是不保存time的
			Timestamp jst = new Timestamp(jud.getTime());
			ps.setObject(index, jst);
		} else {
			ps.setObject(index, value);
		}
	}

	public static Dialect of(String schema) {
		Type type = Type.of(schema);
		switch (type) {
		case MYSQL:
			return new MysqlDialect(type);
		case ORACLE:
			return new OracleDialect(type);
		case POSTGRESQL:
			return new PostgresqlDialect(type);
		case SQL_SERVER_2005:
			return new SqlServer2005Dialect(type);
		case SQL_SERVER_2008:
			return new SqlServer2008Dialect(type);
		case SQL_SERVER_2013:
			return new SqlServer2013Dialect(type);
		case KINGBASE:
			return new KingbaseDialect(type);
		default:
			return new Dialect(type);
		}
	}

	protected static String determineKeyField(List<Rmap> list) {
		if (null == list || list.isEmpty()) return null;
		Rmap msg = list.get(0);
		Object key = msg.key();
		if (null == key) return null;
		return msg.entrySet().stream().filter(e -> key.equals(e.getValue())).map(Map.Entry::getKey).findFirst().orElse(null);
	}
}
