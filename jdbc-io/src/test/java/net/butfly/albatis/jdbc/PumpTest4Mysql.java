package net.butfly.albatis.jdbc;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.pump.Pump;

import java.io.IOException;

public class PumpTest4Mysql {

    public static void main(String[] args) throws IOException {
        JdbcTestInput input = new JdbcTestInput("input");
        String uri = "jdbc:mysql://ph_warning_rhfl_test:Ph_warning_rhfl@test123!@172.16.17.14:3306/ph_warning_rhfl_test?useSSL=false";
        JdbcOutput output = new JdbcOutput("output", new JdbcConnection(new URISpec(uri)));
        Pump pump = Pump.pump(input, 3, output);
        pump.open();

        System.out.println("wait...");
    }
}
