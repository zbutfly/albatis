package net.butfly.albatis.jdbc;

import net.butfly.albatis.io.pump.Pump;

import java.net.URISyntaxException;
import java.sql.SQLException;

public class PumpTest {

    public static void main(String[] args) throws SQLException, URISyntaxException {
        JdbcTestInput input = new JdbcTestInput("input");
        String uri = "jdbc:mysql://ph_warning_rhfl_test:Ph_warning_rhfl@test123!@172.16.17.14:3306/ph_warning_rhfl_test?useSSL=false";
        JdbcOutput output = new JdbcOutput("output", uri, false);
        Pump pump = Pump.pump(input, 3, output);
        pump.open();

        System.out.println("wait...");
    }
}
