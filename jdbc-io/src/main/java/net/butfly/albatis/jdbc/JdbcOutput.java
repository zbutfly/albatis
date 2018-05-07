package net.butfly.albatis.jdbc;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OutputBase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class JdbcOutput extends OutputBase<Message> {
    private final JdbcConnection jc;

    /**
     * constructor with default upsert = true
     * @param name output name
     * @param conn standard jdbc connection
     */
    public JdbcOutput(String name, JdbcConnection conn, String... preExecutedSqlFile) {
        super(name);
        this.jc = conn;
        preExecute(jc, preExecutedSqlFile);
        closing(this::close);
        open();
    }

    @Override
    protected void enqueue0(Sdream<Message> items) {
        AtomicLong n = new AtomicLong(0);
        Map<String, List<Message>> mml = items.list().stream()
            .filter(item -> {
                String table = item.table();
                return null != table && !table.isEmpty();
            }).collect(Collectors.groupingBy(Message::table));
        if (mml.isEmpty()) {
            succeeded(0);
            return;
        }
        try (Connection conn =  jc.client().getConnection()) {
            n.addAndGet(jc.upserter.upsert(mml, conn));
        } catch (SQLException e) {
            logger().error("failed to insert or update items.");
        }
        succeeded(n.get());
    }

    @Override
    public void close() {
        jc.close();
    }

    private void preExecute(JdbcConnection jc, String... files) {
        if (null == files || files.length == 0) {
            logger().info("no sql file need to execute before start JdbcOutput");
            return;
        }
        Connection conn = null;
        try {
            conn = jc.client().getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("for run sql files", e);
        }

        for (String file : files) {
            if (null != file && !file.isEmpty()) executeSqlFile(conn, Paths.get(file));
        }
    }

    private void executeSqlFile(Connection conn, Path path) {
        if (null == conn || null == path) return;
        if (!Files.exists(path)) {
            logger().info("file `" + path + "` not exist");
            return;
        }
        if (!Files.isRegularFile(path)) {
            logger().info("file `" + path + "` is not a regular file");
            return;
        }
        try {
            String sql = Files.lines(path).collect(Collectors.joining(""));
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.execute();
            logger().debug("execute ``````" + sql + "`````` success");
            ps.close();
        } catch (IOException | SQLException e) {
            logger().error("failed to execute sql from file `" + path + "`", e);
        }
    }

}
