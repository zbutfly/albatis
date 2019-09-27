package net.butfly.albatis.hmpp;

import com.hmpp.hmppanalyticsdb.load.ConnInfo;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.utils.JsonUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static net.butfly.albatis.ddl.vals.ValType.Flags.*;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DATE;

public class HmppConnection extends DataConnection<DataSource> {
    ConnInfo connInfo = new ConnInfo();
    public TableDesc[] tables;

    public HmppConnection(URISpec uri) throws IOException {
        super(uri, "hmpp");
        String[] hosts = uri.getHost().split(":");
        connInfo.host = hosts[0];
        connInfo.port = Integer.parseInt(hosts[1]);
        connInfo.dbName = uri.getFile();
        connInfo.userName = uri.getParameter("user");
        connInfo.password = uri.getParameter("password");
        connInfo.schemaName = null == uri.getParameter("schema") ? "public" : uri.getParameter("schema");
        connInfo.encoding = "utf-8";
    }

    @Override
    public HmppOutput outputRaw(TableDesc... table) throws IOException {
        this.tables = table;
        return new HmppOutput(this);
    }

    @Override
    protected DataSource initialize(URISpec uri) {
        try {
            HikariDataSource h = new HikariDataSource(toConfig(uri));
            return h;
        } catch (Throwable t) {
            logger().error("jdbc connection fail on [" + uri.toString() + "]");
            throw new RuntimeException(t);
        }
    }

    public HikariConfig toConfig(URISpec uriSpec) {
        HikariConfig config = new HikariConfig();
        if (null != Configs.gets("albatis.jdbc.maximumpoolsize")
                && !"".equals(Configs.gets("albatis.jdbc.maximumpoolsize"))) {
            config.setMaximumPoolSize(Integer.parseInt(Configs.gets("albatis.jdbc.maximumpoolsize")));
        }
        config.setPoolName("hmppanalyticsdb" + "-Hikari-Pool");
        try {
            Class.forName("com.hmpp.hmppanalyticsdb.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "JDBC driver class [ com.hmpp.hmppanalyticsdb.Driver ] not found, need driver lib jar file?");
        }
        config.setDriverClassName("com.hmpp.hmppanalyticsdb.Driver");
        String jdbcconn = uriSpec.toString().replace("hmpp:"," jdbc:hmppanalyticsdb:");
        logger.info("Connect to jdbc with connection string: \n\t" + jdbcconn);
        config.setJdbcUrl(jdbcconn.split("\\?")[0]);
        if (null == uriSpec.getParameter("user")) {
            config.setUsername(uriSpec.getAuthority().split(":")[0].toString());
            config.setPassword(uriSpec.getAuthority().split(":")[1].substring(0,
                    uriSpec.getAuthority().split(":")[1].lastIndexOf("@")));
        } else {
            config.setUsername(uriSpec.getParameter("username"));
            config.setPassword(uriSpec.getParameter("password"));
        }
        uriSpec.getParameters().forEach(config::addDataSourceProperty);
        return config;
    }

    @Override
    public void construct(Qualifier qualifier, FieldDesc... fields) {
        String sql = buildCreateTableSql(qualifier.name, fields);
        logger().info("Table constructed with statment:\n\t" + sql);
        try (java.sql.Connection conn = client.getConnection(); PreparedStatement ps = conn.prepareStatement(sql.toString());) {
            ps.execute();
        } catch (SQLException e) {
            logger().error("Table construct failed", e);
        }
    }

    @Override
    public void construct(String table, TableDesc tableDesc, List<FieldDesc> fields) {
        String tableName;
        java.sql.Connection conn = null;
        String[] tables = table.split("\\.");
        if (tables.length == 1)
            tableName = tables[0];
        else if (tables.length == 2)
            tableName = tables[1];
        else
            throw new RuntimeException("Please type in correct jdbc table format: db.table !");
        try {
            conn = client.getConnection();
            tableConstruct(conn, tableName, tableDesc, fields);
        } catch (SQLException e) {
            logger().error("construct table failure", e);
        } finally {
            try {
                if (null != conn)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public void tableConstruct(java.sql.Connection conn, String table, TableDesc tableDesc, List<FieldDesc> fields) {
        StringBuilder sb = new StringBuilder();
        List<String> fieldSql = new ArrayList<>();
        for (FieldDesc field : fields)
            fieldSql.add(buildSqlField(tableDesc, field));
        try (Statement statement = conn.createStatement()) {
            List<Map<String, Object>> indexes = tableDesc.indexes;
            sb.append("create table ").append("\"").append(table).append("\"").append("(")
                    .append(String.join(",", fieldSql.toArray(new String[0]))).append(")");
            statement.addBatch(sb.toString());
            if (!indexes.isEmpty()) {
                for (Map<String, Object> index : indexes) {
                    StringBuilder createIndex = new StringBuilder();
                    String type = (String) index.get("type");
                    String alias = (String) index.get("alias");
                    List<String> fieldList = JsonUtils.parseFieldsByJson(index.get("field"));
                    createIndex.append("create ").append(type).append(" ").append(alias).append(" on ").append("\"")
                            .append(table).append("\"").append("(").append("\"")
                            .append(String.join("\",\"", fieldList.toArray(new String[0]))).append("\"").append(")");
                    statement.addBatch(createIndex.toString());
                }
            }
            statement.executeBatch();
            logger().debug("execute ``````" + sb + "`````` success");
        } catch (SQLException e) {
            throw new RuntimeException("create postgre table failure " + e);
        }
    }

    protected String buildSqlField(TableDesc tableDesc, FieldDesc field) {
        StringBuilder sb = new StringBuilder();
        switch (field.type.flag) {
            case INT:
            case SHORT:
            case BINARY:
                sb.append("\"").append(field.name).append("\"").append(" int4");
                break;
            case LONG:
                sb.append("\"").append(field.name).append("\"").append(" int8");
                break;
            case FLOAT:
            case DOUBLE:
                sb.append("\"").append(field.name).append("\"").append(" numeric(10)");
                break;
            case BYTE:
            case BOOL:
                sb.append("\'").append(field.name).append("\'").append(" bool(1)");
                break;
            case STR:
            case CHAR:
            case UNKNOWN:
                sb.append("\"").append(field.name).append("\"").append(" varchar(256)");
                break;
            case DATE:
                sb.append("\"").append(field.name).append("\"").append(" date");
                break;
            default:
                break;
        }
        return sb.toString();
    }

    public String buildCreateTableSql(String table, FieldDesc... fields) {
        StringBuilder sql = new StringBuilder();
        List<String> fieldSql = new ArrayList<>();
        for (FieldDesc f : fields){
            if(f.rowkey)fieldSql.add(buildPostgreField(f));
        }
        for (FieldDesc f : fields)
            if(!f.rowkey)fieldSql.add(buildPostgreField(f));
        sql.append("create table ").append(table).append("(").append(String.join(",", fieldSql.toArray(new String[0])))
                .append(")");
        return sql.toString();
    }

    private String buildPostgreField(FieldDesc field) {
        StringBuilder sb = new StringBuilder();
        switch (field.type.flag) {
            case INT:
            case SHORT:
            case BINARY:
                sb.append(field.name).append(" int4");
                break;
            case LONG:
                sb.append(field.name).append(" int8");
                break;
            case FLOAT:
            case DOUBLE:
                sb.append(field.name).append(" numeric");
                break;
            case BYTE:
            case BOOL:
                sb.append(field.name).append(" bool");
                break;
            case STR:
            case CHAR:
            case UNKNOWN:
                sb.append(field.name).append(" varchar");
                break;
            case DATE:
                sb.append(field.name).append(" date");
                break;
            case TIMESTAMP:
                sb.append(field.name).append(" timestamp");
                break;
            case GEO_PG_GEOMETRY:
                sb.append(field.name).append(" geometry");
                break;
            default:
                break;
        }
        return sb.toString();
    }

    @Override
    public void close() {
        DataSource hds = client;
        if (null != hds && hds instanceof AutoCloseable)
            try {
                ((AutoCloseable) hds).close();
            } catch (Exception e) {
            }
    }


    public static class Driver implements net.butfly.albatis.Connection.Driver<HmppConnection> {
        static {
            DriverManager.register(new Driver());
        }

        @Override
        public HmppConnection connect(URISpec uriSpec) throws IOException {
            return new HmppConnection(uriSpec);
        }

        @Override
        public List<String> schemas() {
            return Colls.list("hmpp");
        }
    }
}
