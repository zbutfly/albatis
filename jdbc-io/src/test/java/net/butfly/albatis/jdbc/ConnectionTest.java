package net.butfly.albatis.jdbc;


import net.butfly.albacore.utils.Config;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

@Config("jdbc-io.properties")
public class ConnectionTest {

    public static void main(String[] args) throws SQLException {
        t1();
    }

    /*mysql*/
    public static void t1() throws SQLException {
        String uri = "jdbc:mysql://47.100.168.59:3306/db1";
        Properties info = new Properties();
        info.putIfAbsent("user", "root");
        info.putIfAbsent("password", "123456");
        try(
//        Connection conn = DriverManager.getConnection(uri, "root", "123456");
        Connection conn = DriverManager.getConnection(uri, info);
        PreparedStatement ps = conn.prepareStatement("select * from goods");
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                System.out.println(rs.getInt("id") + "<--->" + rs.getString("name"));
            }
        }
    }

    /**
     * mysql
     * @throws SQLException
     */
    @Test
    public void t2() throws SQLException {
        String uri = "jdbc:mysql://ph_warning_rhfl_test:Ph_warning_rhfl@test123!@172.16.17.14:3306/ph_warning_rhfl_test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&useSSL=false";
        Connection conn = null;//Jdbcs.getConnection(uri);
        try(PreparedStatement ps  = conn.prepareStatement("select * from WARNING_LEVEL");
        ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                System.out.println("column 1 = " + rs.getObject(1));
            }
        }
    }

    /**
     * oracle
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
    public void t3() throws SQLException, ClassNotFoundException {
        /*String uri = "jdbc:oracle:thin://cominfo_test:cominfo_test1234@172.16.17.14:1521/citest";
        Connection conn = Jdbcs.getConnection(uri);*/
        Class.forName("oracle.jdbc.driver.OracleDriver");
        String uri = "jdbc:oracle:thin:@172.16.17.14:1521/citest";
        String username = "cominfo_test";
        String password = "cominfo_test1234";
        Connection conn = DriverManager.getConnection(uri, username, password);
        try(PreparedStatement ps  = conn.prepareStatement("select * from T0409");
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                System.out.println("column 1 = " + rs.getObject(1));
            }
        }
    }

    /**
     * postgres
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
    public void t4() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        String uri = "jdbc:postgresql://127.0.0.1:5432/postgres";
        String username = "postgres";
        String password = "!@#QAZ123qaz";
        Connection conn = DriverManager.getConnection(uri, username, password);
        try(PreparedStatement ps  = conn.prepareStatement("select * from test");
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                System.out.println("column 1 = " + rs.getObject(1));
            }
        }
    }

    @Test
    public void t5() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        String uri = "jdbc:postgresql://127.0.0.1:5432/postgres";
        String username = "postgres";
        String password = "!@#QAZ123qaz";
        Connection conn = DriverManager.getConnection(uri, username, password);
        String sql = "insert into test set id = 77, name ='def'";
        try(PreparedStatement ps  = conn.prepareStatement(sql)) {
//            ps.setString(1, "test");
            ps.execute();
//            ResultSet rs = ps.executeQuery();
            /*while (rs.next()) {
                System.out.println("column 1 = " + rs.getObject(1));
            }*/
        }
    }
}
