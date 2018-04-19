package net.butfly.albatis.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OutputBase;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class JdbcOutput2 extends OutputBase<Message> {
    private final HikariDataSource hds;
    private final Upserter upserter;

    /**
     * constructor with default upsert = true
     * @param name output name
     * @param uriSpec standard uri spec
     */
    public JdbcOutput2(String name, String uriSpec) {
        this(name, new URISpec(uriSpec));
    }

    /**
     * constructor with default upsert = true
     * @param name output name
     * @param uriSpec standard uri spec
     */
    public JdbcOutput2(String name, URISpec uriSpec) {
        super(name);
        upserter = Upserter.of(uriSpec.getScheme());
        hds = new HikariDataSource(toConfig(upserter, uriSpec));
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
        try (Connection conn = hds.getConnection()) {
            n.addAndGet(upserter.upsert(mml, conn));
        } catch (SQLException e) {
            logger().error("failed to insert or update items.");
        }
        succeeded(n.get());
    }

    @Override
    public void close() {
        if (null != hds && !hds.isClosed()) hds.close();
    }

    private static HikariConfig toConfig(Upserter upserter, URISpec uriSpec) {

        HikariConfig config = new HikariConfig();
        config.setPoolName(upserter.type.name() + "-Hikari-Pool");
        config.setDriverClassName(upserter.type.driver);
        config.setJdbcUrl(upserter.urlAssemble(uriSpec.getScheme(), uriSpec.getHost(), uriSpec.getFile()));
        config.setUsername(uriSpec.getUsername());
        config.setPassword(uriSpec.getPassword());
        uriSpec.getParameters().forEach(config::addDataSourceProperty);
        return config;
    }

}
