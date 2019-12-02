package net.butfly.albatis.jdbc.dialect;

import com.zaxxer.hikari.HikariConfig;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.logger.Logger;

@DialectFor(subSchema = "hive2", jdbcClassname = "com.cloudera.hive.jdbc.HS2Driver")
public class HiveDialect extends Dialect {
	private static final Logger logger = Logger.getLogger(HiveDialect.class);
//	String url = "jdbc:hive2://cdhtest.hik.net:10000/default;Principal=hive/cdhtest.hik.net@HIK.NET;AuthMech=1;KrbRealm=HIK.NET;KrbHostFQDN=cdhtest.hik.net;KrbServiceName=hive;KrbAuthType=0";
	@Override
	public String jdbcConnStr(URISpec uriSpec) {
		return uriSpec.toString();
	}

	@Override
	public HikariConfig toConfig(Dialect dialect, URISpec uriSpec) {
		HikariConfig config = new HikariConfig();
		if (null != Configs.gets("albatis.jdbc.maximumpoolsize")
				&& !"".equals(Configs.gets("albatis.jdbc.maximumpoolsize"))) {
			config.setMaximumPoolSize(Integer.parseInt(Configs.gets("albatis.jdbc.maximumpoolsize")));
		}
        DialectFor d = dialect.getClass().getAnnotation(DialectFor.class);
		config.setPoolName(  "hive-Hikari-Pool");
        if (!"".equals(d.jdbcClassname())) {
            try {
                Class.forName(d.jdbcClassname());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(
                        "JDBC driver class [" + d.jdbcClassname() + "] not found, need driver lib jar file?");
            }
			config.setDriverClassName(d.jdbcClassname());
        }
        String jdbcconn = dialect.jdbcConnStr(uriSpec);
		config.setJdbcUrl(jdbcconn);
		config.addDataSourceProperty("charset","utf-8");
		config.setAutoCommit(false);
		uriSpec.getParameters().forEach(config::addDataSourceProperty);
		return config;
	}
}
