package net.butfly.albatis.jdbc;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albatis.io.R;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 键必须为主键或唯一键
 */
public class PostgresqlUpserter extends Upserter {
    private static final String psql = "insert into %s (%s) values(%s) ON CONFLICT(%s) do update set %s";

    public PostgresqlUpserter(Type type) {
        super(type);
    }

    @Override
    String urlAssemble(URISpec uriSpec) {
        return null;
    }

    @Override
    String urlAssemble(String schema, String host, String database) {
        return schema + "://" + host + "/" + database;
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
                            ps.addBatch(); } catch (SQLException e) {
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
