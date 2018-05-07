package net.butfly.albatis.jdbc;

import com.hzcominfo.albatis.nosql.NoSqlConnection;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.butfly.albacore.io.URISpec;

import java.io.IOException;

public class JdbcConnection extends NoSqlConnection<HikariDataSource> {
    final Upserter upserter;

    public JdbcConnection(URISpec uri) throws IOException {
        super(uri, u -> {
            Upserter upserter = Upserter.of(u.getScheme());
            return new HikariDataSource(toConfig(upserter, u));
        }, "jdbc:mysql", "jdbc:oracle:thin", "jdbc:postgresql", "jdbc:sqlserver", "jdbc:microsoft:sqlserver");
        upserter = Upserter.of(uri.getScheme());
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
        HikariDataSource hds = client();
        if (null != hds && hds.isRunning())
            hds.close();
    }

    @Override
    public JdbcInput input(String... table) throws IOException {
        // TODO: 2018/5/7 unmatched function
        /*JdbcInput input = new JdbcInput();
        input.table(table);
        return input;*/
        return null;
    }

    @Override
    public JdbcOutput output() throws IOException {
        return new JdbcOutput("JdbcOutput", this);
    }
}
