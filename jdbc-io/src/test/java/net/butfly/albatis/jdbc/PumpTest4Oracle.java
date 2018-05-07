package net.butfly.albatis.jdbc;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.pump.Pump;

import java.io.IOException;

public class PumpTest4Oracle {

    public static void main(String[] args) throws IOException {
        JdbcTestInput input = new JdbcTestInput("input");
        String uri = "jdbc:oracle:thin://cominfo_test:cominfo_test1234@172.16.17.14:1521/citest";
        JdbcOutput output = new JdbcOutput("output", new JdbcConnection(new URISpec(uri)));
        Pump pump = Pump.pump(input, 3, output);
        pump.open();

        System.out.println("wait...");
    }
}
