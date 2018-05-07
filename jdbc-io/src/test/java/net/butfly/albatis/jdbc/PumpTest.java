package net.butfly.albatis.jdbc;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.pump.Pump;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;

public class PumpTest {

    @Test
    public void pump4mysql() throws IOException {
        JdbcTestInput input = new JdbcTestInput("input");
        String uri = "jdbc:mysql://ph_warning_rhfl_test:Ph_warning_rhfl@test123!@172.16.17.14:3306/ph_warning_rhfl_test?useSSL=false";
        JdbcOutput output = new JdbcOutput("output", new JdbcConnection(new URISpec(uri)),
            "jdbc-io/src/main/resources/create-table.sql");
        Pump pump = Pump.pump(input, 3, output);
        pump.open();

        System.out.println("wait...");
    }


    @Test
    public void pump4Oracle() throws IOException {
        JdbcTestInput input = new JdbcTestInput("input");
        String uri = "jdbc:oracle:thin://cominfo_test:cominfo_test1234@172.16.17.14:1521/citest";
        JdbcOutput output = new JdbcOutput("output", new JdbcConnection(new URISpec(uri)));
        Pump pump = Pump.pump(input, 3, output);
        pump.open();

        System.out.println("wait...");
    }

    @Test
    public void pump4Postgres() throws IOException {
        JdbcTestInput input = new JdbcTestInput("input");
        String uri = "jdbc:postgresql://test:Test001!@127.0.0.1:5432/postgres";
        JdbcOutput output = new JdbcOutput("output", new JdbcConnection(new URISpec(uri)));
        Pump pump = Pump.pump(input, 3, output);
        pump.open();

        System.out.println("wait...");
    }
}
