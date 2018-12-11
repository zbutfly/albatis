package net.butfly.albatis.jdbc;

public enum Type {
	MYSQL("jdbc:mysql", "com.mysql.cj.jdbc.Driver"), //
	ORACLE("jdbc:oracle:thin", "oracle.jdbc.driver.OracleDriver"), //
	POSTGRESQL("jdbc:postgresql", "org.postgresql.Driver"), //
	SQL_SERVER_2005("jdbc:sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"), //
	SQL_SERVER_2008("jdbc:sqlserver:2008", "com.microsoft.sqlserver.jdbc.SQLServerDriver"), //
	SQL_SERVER_2013("jdbc:sqlserver:2013", "com.microsoft.sqlserver.jdbc.SQLServerDriver"), //
	KINGBASE("jdbc:kingbaseanalyticsdb", "com.kingbase.kingbaseanalyticsdb.ds.KBSimpleDataSource"),
	// DB2("jdbc:db2"),
	// SYBASE("jdbc:sybase:Tds"),
	// INFORMIX("jdbc:informix-sqli")
	JDBC("jdbc", null),;

	private final String schema;
	public final String driver;

	Type(String schema, String driver) {
		this.schema = schema;
		this.driver = driver;
	}

	static Type of(String schema) {
		while (schema.length() > 0) {
			for (Type t : Type.values())
				if (t.schema.equals(schema)) return t;
			schema = schema.substring(schema.lastIndexOf(':'));
		}
		throw new IllegalStateException("schema `" + schema + "` is not support");
	}
}
