package net.butfly.albatis.jdbc;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albatis.io.Message;

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

    @Override
    public String urlAssemble(URISpec uriSpec) {
        return null;
    }

    @Override
    String urlAssemble(String schema, String host, String database) {
        return schema + "://" + host + "/" + database;
    }

    @Override
    public long upsert(Map<String, List<Message>> mml, Connection conn) {
        AtomicLong count = new AtomicLong();
        Exeter.of().join(entry -> {
            mml.forEach((t, l) -> {
                if (l.isEmpty()) return;
                String keyField = determineKeyField(l);
                if (null == keyField) logger().warn("can NOT determine KeyField");
                List<Message> ml = l.stream().sorted((m1, m2) -> m2.size() - m1.size()).collect(Collectors.toList());
                List<String> fl = new ArrayList<>(ml.get(0).keySet());
                String fields = fl.stream().collect(Collectors.joining(", "));
                String values = fl.stream().map(f -> "?").collect(Collectors.joining(", "));
                List<String> ufields = fl.stream().filter(f -> !f.equals(keyField)).collect(Collectors.toList());
                String updates = ufields.stream().map(f -> f + " = ?").collect(Collectors.joining(", "));
                String sql = String.format(psql, t, fields, values, updates);
                logger().debug("MYSQL upsert sql: " + sql);
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    int isize = fl.size();
                    ml.forEach(m -> {
                        try {
                            for (int i = 0; i < fl.size(); i++) {
                                Object value = m.get(fl.get(i));
                                ps.setObject(i + 1, value);
                            }
                            for (int i = 0; i < ufields.size(); i++) {
                                Object value = m.get(ufields.get(i));
                                ps.setObject(isize + i + 1, value);
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
            });
        }, mml.entrySet());
        return count.get();
    }

}
