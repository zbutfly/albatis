package net.butfly.albatis.jdbc;

import net.butfly.albatis.io.pump.Pump;

import java.net.URISyntaxException;
import java.sql.SQLException;

public class PumpTest4Postgres {

    public static void main(String[] args) throws SQLException, URISyntaxException {
        JdbcTestInput input = new JdbcTestInput("input");
        String uri = "jdbc:postgresql://test:Test001!@127.0.0.1:5432/postgres";
        JdbcOutput output = new JdbcOutput("output", uri);
        Pump pump = Pump.pump(input, 3, output);
        pump.open();

        System.out.println("wait...");
    }
}
