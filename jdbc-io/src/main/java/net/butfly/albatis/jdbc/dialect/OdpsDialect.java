package net.butfly.albatis.jdbc.dialect;

import com.zaxxer.hikari.HikariConfig;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.logger.Logger;

@DialectFor(subSchema = "odps:http", jdbcClassname = "com.aliyun.odps.jdbc.OdpsDriver")
public class OdpsDialect extends Dialect {
	private static final Logger logger = Logger.getLogger(OdpsDialect.class);
//	String uri = "jdbc:odps:http://service.cn-hangzhou-zjga-d01.odps.yun.zj/api?project_name=appnull_jgdsj&access_id=yEcmh3twHXiabDFh&access_key=gfrasfsd";
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
		config.setPoolName(d.subSchema() + "-Hikari-Pool");
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
		logger.info("Connect to jdbc with connection string: \n\t" + jdbcconn);
		config.setJdbcUrl(jdbcconn.substring(0, jdbcconn.indexOf("?")));
		config.addDataSourceProperty("charset","utf-8");
		config.setAutoCommit(false);
		uriSpec.getParameters().forEach(config::addDataSourceProperty);
		return config;
	}
}
