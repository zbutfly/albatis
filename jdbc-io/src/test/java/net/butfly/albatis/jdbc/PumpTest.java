package net.butfly.albatis.jdbc;

import net.butfly.albatis.io.pump.Pump;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PumpTest {

    public static void main(String[] args) throws SQLException {
        JdbcTestInput input = new JdbcTestInput("input");
        Connection conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/postgres",
                "postgres", "!@#QAZ123qaz");

        JdbcOutput output = new JdbcOutput("output", conn, false);
        Pump pump = Pump.pump(input, 3, output);
        pump.open();
    }
}
