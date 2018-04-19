package net.butfly.albatis.jdbc;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albatis.io.Message;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public abstract class Upserter implements Loggable {
    public final Type type;

    public Upserter(Type type) {
        this.type = type;
    }

    /**
     * most of the sub class haven impl this func, use {@link Upserter#urlAssemble(String, String, String)} instead
     * @param uriSpec the uri to connect
     * @return url with no parameters
     */
    @Deprecated
    abstract String urlAssemble(URISpec uriSpec);

    /**
     * assemble base url for connecting to RDMS database
     * @param schema database schema
     * @param host host and port
     * @param database default database
     * @return base url
     */
    abstract String urlAssemble(String schema, String host, String database);

    abstract long upsert(Map<String, List<Message>> mml, Connection conn);

    static Upserter of(String schema) {
        Type type = Type.of(schema);
        switch (type) {
            case MYSQL:
                return new MysqlUpserter(type);
            case ORACLE:
                return new OracleUpserter(type);
            case POSTGRESQL:
                return new PostgresqlUpserter(type);
            case SQL_SERVER_2005:
                return new SqlServer2005Upserter(type);
            case SQL_SERVER_2008:
                return new SqlServer2008Upserter(type);
            case SQL_SERVER_2013:
                return new SqlServer2013Upserter(type);
            default:
                throw new IllegalStateException("not supported type: " + type);
        }
    }

    protected static String determineKeyField(List<Message> list) {
        if (null == list || list.isEmpty()) return null;
        Message msg = list.get(0);
        Object key = msg.key();
        if (null == key) return null;
        return msg.entrySet().stream().filter(e -> key.equals(e.getValue())).map(Map.Entry::getKey).findFirst().orElse(null);
    }
}
