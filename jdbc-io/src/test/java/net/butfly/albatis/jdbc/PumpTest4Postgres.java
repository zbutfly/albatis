package net.butfly.albatis.jdbc;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.pump.Pump;

import java.io.IOException;

public class PumpTest4Postgres {

    public static void main(String[] args) throws IOException {
        JdbcTestInput input = new JdbcTestInput("input");
        String uri = "jdbc:postgresql://test:Test001!@127.0.0.1:5432/postgres";
        JdbcOutput output = new JdbcOutput("output", new JdbcConnection(new URISpec(uri)));
        Pump pump = Pump.pump(input, 3, output);
        pump.open();

        System.out.println("wait...");
    }
}
