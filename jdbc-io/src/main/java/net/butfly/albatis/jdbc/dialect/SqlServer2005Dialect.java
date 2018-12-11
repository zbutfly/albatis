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

public class SqlServer2005Dialect extends Dialect {
	private final String psql = "MERGE INTO %s AS T USING (SELECT 1 S) AS S ON (%s) " + " WHEN MATCHED THEN " + " UPDATE SET %s "
			+ " WHEN NOT MATCHED THEN " + " INSERT(%s) VALUES(%s)";

	public SqlServer2005Dialect(Type type) {
		super(type);
	}

	@Override
	protected void doUpsert(Connection conn, String t, String keyField, List<Rmap> l, AtomicLong count) {
		List<Rmap> ml = l.stream().sorted((m1, m2) -> m2.size() - m1.size()).collect(Collectors.toList());
		List<String> fl = new ArrayList<>(ml.get(0).keySet());
		String fields = fl.stream().collect(Collectors.joining(", "));
		String values = fl.stream().map(f -> "?").collect(Collectors.joining(", "));
		String dual = keyField + " = ?";
		List<String> ufields = fl.stream().filter(f -> !f.equals(keyField)).collect(Collectors.toList());
		String updates = ufields.stream().map(f -> f + " = ?").collect(Collectors.joining(", "));
		String sql = String.format(psql, t, dual, fields, values, updates);
		logger().debug("SQLSERVER upsert SQL: " + sql);
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ml.forEach(m -> {
				int offset = 0;
				try {
					{ // set dual
						setObject(ps, offset + 1, m.key());
						offset++;
					}
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

	@Override
	public String jdbcConnStr(URISpec uri) {
		return "jdbc:sqlserver://" + uri.getHost() + ";databaseName=" + uri.getPath(1);
	}
}
