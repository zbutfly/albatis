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

@DialectFor(subSchema = "sqlserver", jdbcClassname = "com.microsoft.sqlserver.jdbc.SQLServerDriver")
public class SqlServer2005Dialect extends Dialect {
	//private final static String UPSERT_SQL_TEMPLATE = "MERGE INTO %s AS T USING (SELECT 1 S) AS S ON (%s = ?) WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT(%s) VALUES(%s)";
	private final static String UPSERT_SQL_TEMPLATE = "UPDATE %s SET %s WHERE %s = ? ; IF(@@ROWCOUNT = 0) BEGIN   INSERT INTO %s (%s)   VALUES(%s)  END";
	private static final String INSERT_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s)";

	@Override
	protected void doInsertOnUpsert(Connection conn, String table, List<Rmap> records, AtomicLong count) {
		records.sort((m1, m2) -> m2.size() - m1.size());
		List<String> allFields = new ArrayList<>(records.get(0).keySet());
		String fieldnames = allFields.stream().collect(Collectors.joining(", "));
		String values = allFields.stream().map(f -> "?").collect(Collectors.joining(", "));
		String sql = String.format(INSERT_SQL_TEMPLATE, table, fieldnames, values);

		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			records.forEach(m -> {
				try {
					for (int i = 0; i < allFields.size(); i++) {
						Object value = m.get(allFields.get(i));
						Dialects.setObject(ps, i + 1, value);
					}
					ps.addBatch();
				} catch (SQLException e) {
					logger().warn(() -> "add `" + m + "` to batch error, ignore this message and continue.", e);
				}
			});
			int[] rs = ps.executeBatch();
			long sucessed = Arrays.stream(rs).filter(r -> r >= 0).count();
			count.addAndGet(sucessed);
		} catch (SQLException e) {
			logger().warn(() -> "execute batch(size: " + records.size() + ") error, operation may not take effect. reason:", e);
		}
	}

	@Override
	protected void doUpsert(Connection conn, String table, String keyField, List<Rmap> records, AtomicLong count) {
		records.sort((m1, m2) -> m2.size() - m1.size());
		List<String> allFields = new ArrayList<>(records.get(0).keySet());
		List<String> nonKeyFields = allFields.stream().filter(f -> !f.equals(keyField)).collect(Collectors.toList());
		String fieldnames = allFields.stream().collect(Collectors.joining(", "));
		String values = allFields.stream().map(f -> "?").collect(Collectors.joining(", "));
		String updates = nonKeyFields.stream().map(f -> f + " = ?").collect(Collectors.joining(", "));
		String sql = String.format(UPSERT_SQL_TEMPLATE, table, updates, keyField, table, fieldnames, values);

		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			records.forEach(m -> {
				int offset = 0;
				try {
					for (int i = 0; i < nonKeyFields.size(); i++) { // set update fields value
						Object value = m.get(nonKeyFields.get(i));
						Dialects.setObject(ps, offset + i + 1, value);
					}
					offset += nonKeyFields.size();

					Dialects.setObject(ps, offset + 1, m.key());
					offset++;
					for (int i = 0; i < allFields.size(); i++) { // set insert fields value
						Object value = m.get(allFields.get(i));
						Dialects.setObject(ps, offset + i + 1, value);
					}
					offset += allFields.size();

					ps.addBatch();
				} catch (SQLException e) {
					logger().warn(() -> "add `" + m + "` to batch error, ignore this message and continue.", e);
				}
			});
			int[] rs = ps.executeBatch();
			long sucessed = Arrays.stream(rs).filter(r -> r >= 0).count();
			count.addAndGet(sucessed);
		} catch (SQLException e) {
			logger().warn(() -> "execute batch(size: " + records.size() + ") error, operation may not take effect. reason:", e);
		}
	}

	@Override
	public String jdbcConnStr(URISpec uri) {
		return "jdbc:sqlserver://" + uri.getHost() + ";databaseName=" + uri.getPath(1);
	}
}
