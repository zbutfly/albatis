package net.butfly.albatis.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class OracleUpsertTest {
    Connection conn;

    @Before
    public void b1() throws SQLException, ClassNotFoundException {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        String url = "jdbc:oracle:thin:@172.16.17.14:1521/citest";
        String username = "cominfo_test";
        String password = "cominfo_test1234";
        conn = DriverManager.getConnection(url, username, password);
        System.out.println("init conn successfully");
    }

    @After
    public void a1() {
        if (null != conn ) try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("close connection");
    }

    @Test
    public void t1() {
        String sql = "select * from ATEST";
        try(PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                System.out.println(rs.getObject(1) + " " + rs.getObject(2) + " " + rs.getObject(3));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t2() {
//        String sql = "insert into ATEST(ID, NAME, ADDRESS) values (3, 'abc', 'xxx')";
//        String sql = "merge into ATEST a using dual on (a = xa) when not matched then insert(ID, NAME, ADDRESS) values (3, 'taidl', 'sdfwe') when matched then update set NAME=taidl', ADDRESS='sdfwe' end";
        String sql = "BEGIN INSERT into ATEST (ID, NAME, ADDRESS) values (3, 'taidl', 'sdfwe') EXCEPTION when dup_val_on_index then update ATEST set NAME=taidl', ADDRESS='sdfwe' where ID = 3 end";
        try(PreparedStatement ps = conn.prepareStatement(sql)) {
            boolean r = ps.execute();
            System.out.println("r = " + r); // true if query else false
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t3() {
        HikariConfig config = new HikariConfig();
        config.setPoolName("Oracle" + "-Hikari-Pool");
        config.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        config.setJdbcUrl("jdbc:oracle:thin:@172.16.17.14:1521/citest");
        config.setUsername("cominfo_test");
        config.setPassword("cominfo_test1234");
        HikariDataSource ds = new HikariDataSource(config);

        try(Connection c = ds.getConnection();
        PreparedStatement ps = c.prepareStatement("MERGE INTO ATEST USING DUAL ON (ID = ?) WHEN NOT MATCHED THEN INSERT (ADDRESS, ID, NAME) VALUES (?, ?, ?) WHEN MATCHED THEN UPDATE SET ADDRESS = ?, NAME = ?");) {
            ps.setObject(1, 1);
            ps.setObject(2, "xxxx");
            ps.setObject(3, 1);
            ps.setObject(4, "yyyy");
            ps.setObject(5, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");
            ps.setObject(6, "yyyyyyyy");
            ps.addBatch();
//            c.commit();
            int[] rs = ps.executeBatch();
            System.out.println("!!!!!!!!!!!          " + Arrays.toString(rs));
//            boolean b = ps.execute();
//            System.out.println(b+ "==============");
//            System.out.println(b+ "==============");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        ds.close();
    }
}
