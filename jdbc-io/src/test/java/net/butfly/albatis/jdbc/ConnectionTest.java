package net.butfly.albatis.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import net.butfly.albatis.ddl.DBDesc;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.ddl.vals.ValType;
import org.junit.Test;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Config;

@Config("jdbc-io.properties")
public class ConnectionTest {

	public static void main(String[] args) throws SQLException {
		t1();
	}

	/* mysql */
	public static void t1() throws SQLException {
		String uri = "jdbc:mysql://47.100.168.59:3306/db1";
		Properties info = new Properties();
		info.putIfAbsent("user", "root");
		info.putIfAbsent("password", "123456");
		try (
				// Connection conn = DriverManager.getConnection(uri, "root", "123456");
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
	 * 
	 * @throws SQLException
	 */
	@Test
	public void t2() throws SQLException {
		String uri = "jdbc:mysql://ph_warning_rhfl_test:Ph_warning_rhfl@test123!@172.16.17.14:3306/ph_warning_rhfl_test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&useSSL=false";
		new URISpec(uri);
		// Jdbcs.getConnection(uri);
		try (Connection conn = DriverManager.getConnection(uri);
				PreparedStatement ps = conn.prepareStatement("select * from WARNING_LEVEL");
				ResultSet rs = ps.executeQuery()) {
			while (rs.next())
				System.out.println("column 1 = " + rs.getObject(1));
		}
	}

	/**
	 * oracle
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	@Test
	public void t3() throws SQLException, ClassNotFoundException {
		/*
		 * String uri = "jdbc:oracle:thin://cominfo_test:cominfo_test1234@172.16.17.14:1521/citest"; Connection conn =
		 * Jdbcs.getConnection(uri);
		 */
		Class.forName("oracle.jdbc.driver.OracleDriver");
		String uri = "jdbc:oracle:thin:@172.16.17.14:1521:citest"; // or String uri = "jdbc:oracle:thin:@172.16.17.14:1521/citest";
		String username = "cominfo_test";
		String password = "cominfo_test1234";
		Connection conn = DriverManager.getConnection(uri, username, password);
		try (PreparedStatement ps = conn.prepareStatement("select * from T0409"); ResultSet rs = ps.executeQuery()) {
			while (rs.next()) {
				System.out.println("column 1 = " + rs.getObject(1));
			}
		}
	}

	/**
	 * postgres
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	@Test
	public void t4() throws SQLException, ClassNotFoundException {
		Class.forName("org.postgresql.Driver");
		String uri = "jdbc:postgresql://127.0.0.1:5432/postgres";
		String username = "test";
		String password = "Test001!";
		Connection conn = DriverManager.getConnection(uri, username, password);
		try (PreparedStatement ps = conn.prepareStatement("select * from test"); ResultSet rs = ps.executeQuery()) {
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
		// insert into "ATEST" (ID, NAME, ADDRESS) values(1,'pg', 'pgpg') ON CONFLICT("ID") do update set "NAME"="EXCLUDED"."NAME";
		String sql = "insert into atest (id, name, address) values(1, 'pg', 'pgpg') ON CONFLICT(id) do update set name=EXCLUDED.name";
		// String sql = "insert into t (id, name) values (3, '9898') ON CONFLICT(id) do update set name=EXCLUDED.name";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			// ps.setString(1, "test");
			ps.execute();
			// ResultSet rs = ps.executeQuery();
			/*
			 * while (rs.next()) { System.out.println("column 1 = " + rs.getObject(1)); }
			 */
		}
	}

	@Test
	public void testLibra() throws IOException {
		String url = "jdbc:postgresql:libra://172.16.17.42:5432/cominfo?user=cominfo&password=cominfo";
		List<FieldDesc> fields = new ArrayList<>();
		DBDesc dbDesc = DBDesc.of("cominfo", url);
		TableDesc tableDesc = dbDesc.table("test_libra");
//		List<List<String>> keys = new ArrayList<>();
		List<String> key = new ArrayList<>();
		key.add("id");
		tableDesc.keys.add(key);
		ValType type1 = ValType.of("int");
		ValType type2 = ValType.of("string");
		FieldDesc f1 = new FieldDesc(tableDesc, "id", type1);
		FieldDesc f2 = new FieldDesc(tableDesc, "name", type2);
		fields.add(f1);
		fields.add(f2);
		try (JdbcConnection connection = new JdbcConnection(new URISpec(url));) {
			connection.construct("test_libra", tableDesc, fields);
		}
	}
}
