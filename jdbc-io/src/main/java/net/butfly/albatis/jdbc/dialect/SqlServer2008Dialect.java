package net.butfly.albatis.jdbc.dialect;

import net.butfly.albatis.jdbc.dialect.Dialect.DialectFor;

@DialectFor(subSchema = "jdbc:sqlserver:2008")
public class SqlServer2008Dialect extends SqlServer2005Dialect {}
