package net.butfly.albatis.jdbc;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albatis.io.Rmap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class MysqlUpserter extends Upserter {
	private static final String psql = "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s";

	public MysqlUpserter(Type type) {
		super(type);
	}

	@Deprecated
	@Override
	public String urlAssemble(URISpec uriSpec) {
		return null;
	}

	@Override
	String urlAssemble(String schema, String host, String database) {
		return schema + "://" + host + "/" + database;
	}

	protected String quota(String f) {
		return '`' == f.charAt(0) && '`' == f.charAt(f.length() - 1) ? f : "`" + f + "`";
	}

	@Override
	public long upsert(Map<String, List<Rmap>> mml, Connection conn) {
		AtomicLong count = new AtomicLong();
		Exeter.of().join(entry -> {
			if (entry.getValue().isEmpty()) return;
			String keyField = determineKeyField(entry.getValue());
			if (null == keyField) logger().warn("can NOT determine KeyField");
			List<Rmap> ml = entry.getValue().stream().sorted((m1, m2) -> m2.size() - m1.size()).collect(Collectors.toList());
			for (int j = 0; j < ml.size(); j++) {
				List<String> fl = new ArrayList<>(ml.get(j).keySet());
				String fields = fl.stream().map(this::quota).collect(Collectors.joining(", "));
				String values = fl.stream().map(f -> "?").collect(Collectors.joining(", "));
				List<String> ufields = fl.stream().filter(f -> !f.equals(keyField)).collect(Collectors.toList());
				String updates = ufields.stream().map(f -> quota(f) + " = ?").collect(Collectors.joining(", "));
				String sql = String.format(psql, entry.getKey(), fields, values, updates);
				logger().debug("MYSQL upsert sql: " + sql);
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					int isize = fl.size();
					try {
						for (int i = 0; i < fl.size(); i++) {
							Object value = ml.get(j).get(fl.get(i));
							setObject(ps, i + 1, value);
						}
						for (int i = 0; i < ufields.size(); i++) {
							Object value = ml.get(j).get(ufields.get(i));
							setObject(ps, isize + i + 1, value);
						}
						ps.addBatch();
					} catch (SQLException e) {
						logger().warn(() -> "add `" + entry.getValue() + "` to batch error, ignore this message and continue.", e);
					}
					int[] rs = ps.executeBatch();
					long sucessed = Arrays.stream(rs).filter(r -> r >= 0).count();
					count.set(sucessed);
				} catch (SQLException e) {
					logger().warn(() -> "execute batch(size: " + entry.getValue().size()
							+ ") error, operation may not take effect. reason:", e);
				}
			}
		}, mml.entrySet());
		return count.get();
	}

}
