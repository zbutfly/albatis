package net.butfly.albatis.jdbc.dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.jdbc.Type;

public class OracleDialect extends Dialect {
	private static final String psql = "MERGE INTO %s USING DUAL ON (%s) WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s) WHEN MATCHED THEN UPDATE SET %s";

	public OracleDialect(Type type) {
		super(type);
	}

	@Override
	protected void doUpsert(Connection conn, String t, String keyField, List<Rmap> l, AtomicLong count) {
		List<Rmap> ml = l.stream().sorted((m1, m2) -> m2.size() - m1.size()).collect(Collectors.toList());
		List<String> fl = new ArrayList<>(ml.get(0).keySet());
		String fields = fl.stream().collect(Collectors.joining(", "));
		String values = fl.stream().map(f -> "?").collect(Collectors.joining(", "));
		List<String> ufields = fl.stream().filter(f -> !f.equals(keyField)).collect(Collectors.toList());
		String updates = ufields.stream().map(f -> f + " = ?").collect(Collectors.joining(", "));
		String dual = keyField + " = ?";
		String sql = String.format(psql, t, dual, fields, values, updates);
		logger().debug("ORACLE upsert SQL: " + sql);
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ml.forEach(m -> {
				int offset = 0;
				try {
					setObject(ps, offset + 1, m.key());
					offset++;
					for (int i = 0; i < fl.size(); i++) { // set insert fields value
						Object value = m.get(fl.get(i));
						setObject(ps, offset + i + 1, value);
					}
					offset += fl.size();
					for (int i = 0; i < ufields.size(); i++) { // set update fields value
						Object value = m.get(ufields.get(i));
						setObject(ps, offset + i + 1, value);
					}
					offset += ufields.size();
					ps.addBatch();
				} catch (SQLException e) {
					logger().warn(() -> "add `" + m + "` to batch error, ignore this message and continue." + e.getSQLState());
				}
			});
			int[] rs = ps.executeBatch();
			long sucessed = Arrays.stream(rs).filter(r -> r >= 0).count();
			count.set(sucessed);
		} catch (SQLException e) {
			logger().warn(() -> "execute batch(size: " + l.size() + ") error, operation may not take effect. reason:", e);
		}
	}

	// : means sid(!sid), / means sid
	@Override
	public String jdbcConnStr(URISpec uri) {
		String db = uri.getPath(1);
		return uri.getScheme() + ":@" + uri.getHost() + (db.charAt(0) == '!' ? (":" + db.substring(1)) : ("/" + db));
	}
}
