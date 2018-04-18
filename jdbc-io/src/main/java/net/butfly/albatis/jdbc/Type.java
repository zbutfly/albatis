package net.butfly.albatis.jdbc;

import java.util.Arrays;
import java.util.Optional;

public enum Type {
    MYSQL("jdbc:mysql", "com.mysql.cj.jdbc.Driver"),
    ORACLE("jdbc:oracle:thin", "oracle.jdbc.driver.OracleDriver"),
    POSTGRESQL("jdbc:postgresql", "org.postgresql.Driver"),
    SQL_SERVER_2005("jdbc:sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
    SQL_SERVER_2008("jdbc:microsoft:sqlserver", "com.microsoft.jdbc.sqlserver.SQLServerDriver"),
    SQL_SERVER_2013("jdbc:sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
//    DB2("jdbc:db2"),
//    SYBASE("jdbc:sybase:Tds"),
//    INFORMIX("jdbc:informix-sqli")
    ;

    private final String schema;
    public final String driver;

    Type(String schema, String driver) {
        this.schema = schema;
        this.driver = driver;
    }

    static Type of(String schema) {
        Optional<Type> ot = Arrays.stream(Type.values()).filter(type -> type.schema.equals(schema)).findFirst();
        if (ot.isPresent()) return ot.get();
        throw new IllegalStateException("schema `" + schema + "` is not support");
    }
}
