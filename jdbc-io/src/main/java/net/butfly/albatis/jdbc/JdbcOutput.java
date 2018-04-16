package net.butfly.albatis.jdbc;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OutputBase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class JdbcOutput extends OutputBase<Message> {
    private static final Logger log = Logger.getLogger(JdbcOutput.class);
    private final Connection conn;
    private final boolean upsert;
    private static final String psql = "INSERT INTO %s (%s) VALUES (%s)";

    public JdbcOutput(String name, Connection conn) {
        this(name, conn, true);
    }

    protected JdbcOutput(String name, Connection connection, boolean upsert) {
        super(name);
        if (null == connection) {
            throw new NullPointerException("connection");
        }
        this.conn = connection;
        this.upsert = upsert;
        open();
    }

    @Override
    protected void enqueue0(Sdream<Message> items) {
        AtomicLong n = new AtomicLong();
        if (upsert) {
            //n.set(items.map(m -> conn.collection(m.table()).save(MongoConnection.dbobj(m)).getN()).reduce(Lambdas.sumInt()));
            System.out.println("into upsert......................................do nothing");
        } else {
            Map<String, List<Message>> mml = items.list().stream()
                    .filter(item -> {
                        String table = item.table();
                        return null != table && !table.isEmpty();
                    }).collect(Collectors.groupingBy(Message::table));
            if (mml.isEmpty()) return;
            Exeter.of().join(entry -> {
                System.out.println("entry: " + entry);
                mml.forEach((t, l) -> {
                    if (l.isEmpty()) return;
                List<Message> ml = l.stream().sorted((m1, m2) -> m2.size() - m1.size()).collect(Collectors.toList());
                    List<String> fl = new ArrayList<>(ml.get(0).keySet());
                    String fields = fl.stream().collect(Collectors.joining(", "));
                    String values = fl.stream().map(f -> "?").collect(Collectors.joining(", "));
                    String sql = String.format(psql, t, fields, values);
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ml.forEach(m -> {
                            for (int i = 0; i < fl.size(); i++) {
                                Object value = m.get(fl.get(i));
                                try { ps.setObject(i + 1, value); } catch (SQLException e) {
                                    log.warn(() -> "add `" + value + "` to sql parameter error, ignore this message and continue.", e);
                                }
                            }
                            try { ps.addBatch(); } catch (SQLException e) {
                                log.warn(() -> "add `" + m + "` to batch error, ignore this message and continue.", e);
                            }
                        });
                        int[] rs = ps.executeBatch();
                        long sucessed = Arrays.stream(rs).filter(r -> r >= 0).count();
                        n.addAndGet(sucessed);
                    } catch (SQLException e) {
                        log.warn(() -> "execute batch(size: " + l.size() + ") error, operation may not take effect. reason:", e);
                    }
                });
            }, mml.entrySet());
        }
        succeeded(n.get());
    }
}
