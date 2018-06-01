package net.butfly.albatis.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albatis.io.R;

public class SqlServer2005Upserter extends Upserter {
	private final String psql = "MERGE INTO %s AS T USING (SELECT 1 S) AS S ON (%s) "
			+ " WHEN MATCHED THEN "
			+ " UPDATE SET %s "
			+ " WHEN NOT MATCHED THEN "
			+ " INSERT(%s) VALUES(%s)";
    public SqlServer2005Upserter(Type type) {
        super(type);
    }

    @Override
    String urlAssemble(URISpec uriSpec) {
        return null;
    }

    @Override
    String urlAssemble(String schema, String host, String database) {
    	return schema + "://" + host + ";databaseName=" + database;
    }
    
    @Override
    long upsert(Map<String, List<R>> mml, Connection conn) {
    	AtomicLong count = new AtomicLong();
        Exeter.of().join(entry -> {
            mml.forEach((t, l) -> {
                if (l.isEmpty()) return;
                String keyField = determineKeyField(l);
                if (null == keyField) logger().warn("can NOT determine KeyField");
                List<R> ml = l.stream().sorted((m1, m2) -> m2.size() - m1.size()).collect(Collectors.toList());
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
            });
        }, mml.entrySet());
        return count.get();
    }

}
