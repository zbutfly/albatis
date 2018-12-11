package net.butfly.albatis.jdbc.dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import net.butfly.albatis.io.Rmap;

import net.butfly.albatis.jdbc.dialect.Dialect.DialectFor;

@DialectFor(subSchema = "postgresql", jdbcClassname = "org.postgresql.Driver")
public class PostgresqlDialect extends Dialect {
	private static final String psql = "insert into %s (%s) values(%s) ON CONFLICT(%s) do update set %s";

	@Override
	protected void doUpsert(Connection conn, String t, String keyField, List<Rmap> l, AtomicLong count) {
		List<Rmap> ml = l.stream().sorted((m1, m2) -> m2.size() - m1.size()).collect(Collectors.toList());
		List<String> fl = new ArrayList<>(ml.get(0).keySet());
		String fields = fl.stream().collect(Collectors.joining(", "));
		String values = fl.stream().map(f -> "?").collect(Collectors.joining(", "));
		List<String> ufields = fl.stream().filter(f -> !f.equals(keyField)).collect(Collectors.toList()); // update fields
		String updates = ufields.stream().map(f -> f + " = EXCLUDED." + f).collect(Collectors.joining(", "));
		String sql = String.format(psql, t, fields, values, keyField, updates);
		logger().debug("POSTGRES upsert sql: " + sql);
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ml.forEach(m -> {
				try {
					for (int i = 0; i < fl.size(); i++) {
						Object value = m.get(fl.get(i));
						setObject(ps, i + 1, value);
					}
					ps.addBatch();
				} catch (SQLException e) {
					logger().warn(() -> "add `" + m + "` to batch error, ignore this message and continue.", e);
				}
			});
			int[] rs = ps.executeBatch();
			long sucessed = Arrays.stream(rs).filter(r -> r >= 0).count();
			count.set(sucessed);
		} catch (SQLException e) {
			logger().warn(() -> "execute batch(size: " + l.size() + ") error, operation may not take effect. reason:", e);
		}
	}
}
