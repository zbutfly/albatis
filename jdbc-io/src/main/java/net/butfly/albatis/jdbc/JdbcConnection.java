package net.butfly.albatis.jdbc;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import com.hzcominfo.albatis.nosql.NoSqlConnection;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.Field;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableCustomSet;
import net.butfly.albatis.ddl.TableDesc;

import static net.butfly.albatis.ddl.vals.ValType.Flags.*;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BYTE;

public class JdbcConnection extends NoSqlConnection<DataSource> {
    final Upserter upserter;

    public JdbcConnection(URISpec uri) throws IOException {
        super(uri, "jdbc");
        upserter = Upserter.of(uri.getScheme());
    }

    @Override
    protected DataSource initialize(URISpec uri) {
        Upserter upserter = Upserter.of(uri.getScheme());
        return new HikariDataSource(toConfig(upserter, uri));
    }

    @Override
    public void construct(String table, FieldDesc... fields) {
        try (Connection conn = client.getConnection();) {
            if (uri.getScheme().startsWith("jdbc:oracle")) {
                StringBuilder sql = new StringBuilder();
                List<String> fieldSql = new ArrayList<>();
                for (FieldDesc f : fields)
                    fieldSql.add(buildField(f));
                sql.append("create table ").append(table).append("(")
                        .append(String.join(",", fieldSql.toArray(new String[0]))).append(")");
                try (PreparedStatement ps = conn.prepareStatement(sql.toString());) {
                    ps.execute();
                    logger().info("Table constructed by:\n\t" + sql);
                } catch (SQLException e) {
                    logger.error("Table construct failed", e);
                }
            } else
                throw new UnsupportedOperationException("Jdbc table create not supported for:" + uri.getScheme());
        } catch (SQLException e1) {
        }
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

    @Override
    public void close() {
        DataSource hds = client;
        if (null != hds && hds instanceof AutoCloseable)
            try {
                ((AutoCloseable) hds).close();
            } catch (Exception e) {
            }
    }

    @Override
    public JdbcInput input(TableDesc... sql) throws IOException {
        if (sql.length > 1)
            throw new UnsupportedOperationException("Multiple sql input");
        JdbcInput i;
        try {
            i = new JdbcInput("JdbcInput", this);
            i.query("select * from " + sql);
        } catch (SQLException e) {
            throw new IOException(e);
        }
        return i;
    }

    @Override
    public JdbcOutput output(TableDesc... table) throws IOException {
        return new JdbcOutput("JdbcOutput", this);
    }

    public static class Driver implements com.hzcominfo.albatis.nosql.Connection.Driver<JdbcConnection> {
        static {
            DriverManager.register(new Driver());
        }

        @Override
        public JdbcConnection connect(URISpec uriSpec) throws IOException {
            return new JdbcConnection(uriSpec);
        }

        @Override
        public List<String> schemas() {
            return Colls.list("jdbc");
        }
    }

    private static String buildField(FieldDesc field) {
        StringBuilder sb = new StringBuilder();
        switch (field.type.flag) {
            case INT:
                sb.append(field.name).append(" number(32, 0)");
                break;
            case LONG:
                sb.append(field.name).append(" number(64, 0)");
                break;
            case STR:
                sb.append(field.name).append(" varchar2(100)");
                break;
            case DATE:
                sb.append(field.name).append(" date");
                break;
            default:
                break;
        }
        if (field.rowkey)
            sb.append(" not null primary key");
        return sb.toString();
    }

    /**
     * mysql create table
     *
     * @param url
     * @param table
     * @param fields
     * @param tableCustomSet
     */
    public void createMysqlTable(String url, String table, List<Field> fields, TableCustomSet tableCustomSet) {
        try (JdbcConnection jdbcConnection = new JdbcConnection(new URISpec(url));
             Connection conn = jdbcConnection.client.getConnection()) {
            StringBuilder sb = new StringBuilder();
            List<String> fieldSql = new ArrayList<>();
            for (Field field : fields)
                fieldSql.add(buildSqlField(field, tableCustomSet));
            sb.append("create table ").append(table).append("(").append(String.join(",", fieldSql.toArray(new String[0]))).append(")");
            PreparedStatement ps = conn.prepareStatement(sb.toString());
            ps.execute();
            logger().debug("execute ``````" + sb + "`````` success");
        } catch (SQLException | IOException e) {
            throw new RuntimeException("create sql table failure  " + e);
        }
    }

    /**
     * oracle create table
     *
     * @param url
     * @param table
     * @param fields
     * @param tableCustomSet
     */
    public void createOracleTable(String url, String table, List<Field> fields, TableCustomSet tableCustomSet) {
        try (JdbcConnection jdbcConnection = new JdbcConnection(new URISpec(url));
             Connection conn = jdbcConnection.client.getConnection()) {
            StringBuilder sb = new StringBuilder();
            List<String> fieldSql = new ArrayList<>();
            for (Field field : fields)
                fieldSql.add(buildOracleField(field, tableCustomSet));
            sb.append("create table ").append(table).append("(").append(String.join(",", fieldSql.toArray(new String[0]))).append(")");
            PreparedStatement ps = conn.prepareStatement(sb.toString());
            ps.execute();
            logger().debug("execute ``````" + sb + "`````` success");
        } catch (SQLException | IOException e) {
            throw new RuntimeException("create oracle table failure " + e);
        }
    }

    private static String buildSqlField(Field field, TableCustomSet tableCustomSet) {
        StringBuilder sb = new StringBuilder();
        switch (field.getType().flag) {
            case INT:
                sb.append(field.getFieldName()).append(" int(16)");
                break;
            case BOOL:
            case BYTE:
                sb.append(field.getFieldName()).append(" tinyint(1)");
                break;
            case STR:
                sb.append(field.getFieldName()).append(" varchar(256)");
                break;
            case DATE:
                sb.append(field.getFieldName()).append(" datetime");
                break;
            default:
                break;
        }
        if (tableCustomSet.getKeys().get(0).contains(field.getFieldName())) sb.append(" not null primary key");
        return sb.toString();
    }

    private static String buildOracleField(Field field, TableCustomSet tableCustomSet) {
        StringBuilder sb = new StringBuilder();
        switch (field.getType().flag) {
            case INT:
                sb.append(field.getFieldName()).append(" number(32)");
                break;
            case LONG:
                sb.append(field.getFieldName()).append(" number(64)");
                break;
            case STR:
                sb.append(field.getFieldName()).append(" varchar2(100)");
                break;
            case DATE:
                sb.append(field.getFieldName()).append(" date");
                break;
            default:
                break;
        }
        if (tableCustomSet.getKeys().get(0).contains(field.getFieldName())) sb.append(" not null primary key");
        return sb.toString();
    }

    /***
     * judge jdbc whether create table
     *
     * @param url
     * @param table
     * @return
     */
    public boolean judgeJDBC(String url, String table) {
        try (JdbcConnection jdbcConnection = new JdbcConnection(new URISpec(url));
             Connection conn = jdbcConnection.client.getConnection()) {
            DatabaseMetaData dbm = conn.getMetaData();
            ResultSet rs = dbm.getTables(null, null, table, null);
            if (rs.next()) {
                return true;
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
        return false;
    }


}
