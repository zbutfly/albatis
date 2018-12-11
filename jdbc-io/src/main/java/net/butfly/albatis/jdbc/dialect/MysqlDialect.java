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

@DialectFor(subSchema = "mysql", jdbcClassname = "com.mysql.cj.jdbc.Driver")
public class MysqlDialect extends Dialect {
	private static final String UPSERT_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s";

	@Override
	protected void doInsertOnUpsert(Connection conn, String table, List<Rmap> records, AtomicLong count) {
		records.forEach(r -> {
			List<String> allFields = new ArrayList<>(r.keySet());
			List<String> nonKeyFields = allFields.stream()/* .filter(f -> !f.equals(keyField)) */.collect(Collectors.toList());

			String fieldnames = allFields.stream().map(this::quota).collect(Collectors.joining(", "));
			String values = allFields.stream().map(f -> "?").collect(Collectors.joining(", "));
			String updates = nonKeyFields.stream().map(f -> quota(f) + " = ?").collect(Collectors.joining(", "));
			String sql = String.format(UPSERT_SQL_TEMPLATE, table, fieldnames, values, updates);

			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				int isize = allFields.size();
				try {
					for (int i = 0; i < allFields.size(); i++) {
						Object value = r.get(allFields.get(i));
						Dialects.setObject(ps, i + 1, value);
					}
					for (int i = 0; i < nonKeyFields.size(); i++) {
						Object value = r.get(nonKeyFields.get(i));
						Dialects.setObject(ps, isize + i + 1, value);
					}
					ps.addBatch();
				} catch (SQLException e) {
					logger().warn(() -> "add `" + records + "` to batch error, ignore this message and continue.", e);
				}
				int[] rs = ps.executeBatch();
				count.addAndGet(Arrays.stream(rs).filter(rr -> rr >= 0).count());
			} catch (SQLException e) {
				logger().warn(() -> "execute batch(size: " + records.size() + ") error, operation may not take effect. reason:", e);
			}
		});
	}

	protected String quota(String f) {
		return '`' == f.charAt(0) && '`' == f.charAt(f.length() - 1) ? f : "`" + f + "`";
	}
}
