package net.butfly.albatis.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OutputBase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class JdbcOutput extends OutputBase<Message> {
    private static final Logger log = Logger.getLogger(JdbcOutput.class);
    private final HikariDataSource hds;
    private final boolean upsert;
    private static final String psql = "INSERT INTO %s (%s) VALUES (%s)";

    /**
     * constructor with default upsert = true
     * @param name output name
     * @param uriSpec standard uri spec
     */
    public JdbcOutput(String name, String uriSpec) {
        this(name, uriSpec, true);
    }

    /**
     * constructor with default upsert = true
     * @param name output name
     * @param uriSpec standard uri spec
     */
    public JdbcOutput(String name, URISpec uriSpec) {
        this(name, uriSpec, true);
    }

    public JdbcOutput(String name, String uriSpec, boolean upsert) {
        this(name, new URISpec(uriSpec), upsert);
    }

    public JdbcOutput(String name, URISpec uriSpec, boolean upsert) {
        super(name);
        hds = new HikariDataSource(toConfig(uriSpec));
        this.upsert = upsert;
        this.closing(this::close);
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
                    try (Connection conn = hds.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
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

    @Override
    public void close() {
        if (null != hds && hds.isRunning()) hds.close();
    }

    private static HikariConfig toConfig(URISpec uriSpec) {
        assert null != uriSpec;
        Type type = Type.of(uriSpec.getScheme());

        HikariConfig config = new HikariConfig();
        config.setPoolName(type.name() + "-Hikari-Pool");
        config.setDriverClassName(type.driverClass);
        config.setJdbcUrl(urlAssemble(type, uriSpec));
        config.setUsername(uriSpec.getUsername());
        config.setPassword(uriSpec.getPassword());
        uriSpec.getParameters().forEach(config::addDataSourceProperty);
        return config;
    }

    private static String urlAssemble(Type type, URISpec uriSpec) {
        return type.schema + type.sd + uriSpec.getHost() + type.dd + uriSpec.getFile();
    }

    private static enum Type {

        ORACLE("jdbc:oracle:thin", 1521, "oracle.jdbc.driver.OracleDriver", ":@", ":"),
        MYSQL("jdbc:mysql", 3306, "com.mysql.cj.jdbc.Driver"),
        DB2("jdbc:db2", 50000, "com.ibm.db2.jcc.DB2Driver"),
        SYBASE("jdbc:sybase:Tds", 5007, "com.sybase.jdbc.SybDriver", ":", "/"),
        POSTGRESQL("jdbc:postgresql", 5432, "org.postgresql.Driver"),
        SQL_SERVER_2005("jdbc:sqlserver", 1433, "com.microsoft.sqlserver.jdbc.SQLServerDriver", "://", ";databasename="),
        SQL_SERVER_2008("jdbc:microsoft:sqlserver", 1433, "com.microsoft.jdbc.sqlserver.SQLServerDriver", "://", ";DatabaseName="),
        SQL_SERVER_2013("jdbc:sqlserver", 1433, "com.microsoft.sqlserver.jdbc.SQLServerDriver", "://", ";databasename="),
        INFORMIX("jdbc:informix-sqli", 1533, "com.informix.jdbc.IfxDriver");

        private String schema;
        private int defaultPort;
        private String driverClass;
        private String sd, dd;

        private Type(String schema, int defaultPort, String driverClass) {
            this(schema, defaultPort, driverClass, "://", "/");
        }

        private Type(String schema, int defaultPort, String driverClass, String schemaDelimiter, String dbDelimiter) {
            this.schema = schema;
            this.defaultPort = defaultPort;
            this.driverClass = driverClass;
            this.sd = schemaDelimiter;
            this.dd = dbDelimiter;
        }

        public static Type of(String schema) {
            Optional<Type> ot = Arrays.stream(Type.values()).filter(t -> t.schema.equals(schema)).findFirst();
            if (ot.isPresent()) return ot.get();
            throw new RuntimeException("not supported schema: " + schema);
        }

        @Override
        public String toString() {
            return String.format("[type=%s, schema=%s, defaultPort=%d, driver=%s]", this.name(), schema, defaultPort, driverClass);
        }
    }
}
