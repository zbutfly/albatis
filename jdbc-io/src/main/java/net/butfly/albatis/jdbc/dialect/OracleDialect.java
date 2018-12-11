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

@DialectFor(subSchema = "oracle:thin", jdbcClassname = "oracle.jdbc.driver.OracleDriver")
public class OracleDialect extends Dialect {
	private static final String UPSERT_SQL_TEMPLATE = "MERGE INTO %s USING DUAL ON (%s = ?) WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s) WHEN MATCHED THEN UPDATE SET %s";

	@Override
	protected void doUpsert(Connection conn, String table, String keyField, List<Rmap> records, AtomicLong count) {
		records.sort((m1, m2) -> m2.size() - m1.size());
		List<String> allFields = new ArrayList<>(records.get(0).keySet());
		List<String> nonKeyFields = allFields.stream().filter(f -> !f.equals(keyField)).collect(Collectors.toList());

		String fieldnames = allFields.stream().collect(Collectors.joining(", "));
		String values = allFields.stream().map(f -> "?").collect(Collectors.joining(", "));
		String updates = nonKeyFields.stream().map(f -> f + " = ?").collect(Collectors.joining(", "));
		String sql = String.format(UPSERT_SQL_TEMPLATE, table, keyField, fieldnames, values, updates);

		logger().debug("ORACLE upsert SQL: " + sql);
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			records.forEach(m -> {
				int offset = 0;
				try {
					Dialects.setObject(ps, offset + 1, m.key());
					offset++;
					for (int i = 0; i < allFields.size(); i++) { // set insert fields value
						Object value = m.get(allFields.get(i));
						Dialects.setObject(ps, offset + i + 1, value);
					}
					offset += allFields.size();
					for (int i = 0; i < nonKeyFields.size(); i++) { // set update fields value
						Object value = m.get(nonKeyFields.get(i));
						Dialects.setObject(ps, offset + i + 1, value);
					}
					offset += nonKeyFields.size();
					ps.addBatch();
				} catch (SQLException e) {
					logger().warn(() -> "add `" + m + "` to batch error, ignore this message and continue." + e.getSQLState());
				}
			});
			int[] rs = ps.executeBatch();
			long sucessed = Arrays.stream(rs).filter(r -> r >= 0).count();
			count.set(sucessed);
		} catch (SQLException e) {
			logger().warn(() -> "execute batch(size: " + records.size() + ") error, operation may not take effect. reason:", e);
		}
	}

	// : means sid(!sid), / means sid
	@Override
	public String jdbcConnStr(URISpec uri) {
		String db = uri.getPath(1);
		if ("".equals(db)) db = uri.getFile();
		return uri.getScheme() + ":@" + uri.getHost() + (db.charAt(0) == '!' ? (":" + db.substring(1)) : ("/" + db));
	}
}
