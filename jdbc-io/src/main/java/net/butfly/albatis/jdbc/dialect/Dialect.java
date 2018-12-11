package net.butfly.albatis.jdbc.dialect;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import net.butfly.albacore.exception.NotImplementedException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.jdbc.dialect.Dialect.DialectFor;

@DialectFor
public class Dialect implements Loggable {
	final protected Logger logger = Logger.getLogger(this.getClass());

	public Dialect() {}

	public String subSchhema() {
		return "";
	}

	public String jdbcClassname() {
		return null;
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

	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	@Documented
	public @interface DialectFor {
		String subSchema() default "";

		String jdbcClassname() default "";
	}

	public static Dialect of(String schema) {
		String s = schema;
		Set<Class<? extends Dialect>> classes = Reflections.getSubClasses(Dialect.class, "net.butfly.albatis.jdbc.dialect");
		while (s.indexOf(":") > 0) {
			s = s.substring(s.indexOf(":") + 1);
			for (Class<? extends Dialect> c : classes)
				if (c.getAnnotation(DialectFor.class).subSchema().equals(s)) //
					return Reflections.construct(c);
		}
		return new Dialect();
	}

	protected static String determineKeyField(List<Rmap> list) {
		if (null == list || list.isEmpty()) return null;
		Rmap msg = list.get(0);
		Object key = msg.key();
		if (null == key) return null;
		return msg.entrySet().stream().filter(e -> key.equals(e.getValue())).map(Map.Entry::getKey).findFirst().orElse(null);
	}
}
