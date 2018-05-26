package net.butfly.albatis.jdbc;

import static net.butfly.albatis.ddl.vals.ValType.Flags.DATE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.INT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.LONG;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STR;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.hzcominfo.albatis.nosql.NoSqlConnection;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.FieldDesc;

public class JdbcConnection extends NoSqlConnection<HikariDataSource> {
	final Upserter upserter;

	public JdbcConnection(URISpec uri) throws IOException {
		super(uri, u -> {
			Upserter upserter = Upserter.of(u.getScheme());
			return new HikariDataSource(toConfig(upserter, u));
		}, "jdbc:mysql", "jdbc:oracle:thin", "jdbc:postgresql", "jdbc:sqlserver", "jdbc:microsoft:sqlserver");
		upserter = Upserter.of(uri.getScheme());
	}

	@Override
	public void construct(String table, FieldDesc... fields) {
		try (Connection conn = client().getConnection();) {
			if (uri.getScheme().startsWith("jdbc:oracle")) {
				StringBuilder sql = new StringBuilder();
				List<String> fieldSql = new ArrayList<>();
				for (FieldDesc f : fields)
					fieldSql.add(buildField(f));
				sql.append("create table ").append(table).append("(").append(String.join(",", fieldSql.toArray(new String[0]))).append(")");
				try (PreparedStatement ps = conn.prepareStatement(sql.toString());) {
					ps.execute();
					logger().debug("execute ``````" + sql + "`````` success");
				} catch (SQLException e) {}
			} else throw new UnsupportedOperationException("Jdbc table create not supported for:" + uri.getScheme());
		} catch (SQLException e1) {}
	}

	private static HikariConfig toConfig(Upserter upserter, URISpec uriSpec) {
		HikariConfig config = new HikariConfig();
		config.setPoolName(upserter.type.name() + "-Hikari-Pool");
		config.setDriverClassName(upserter.type.driver);
		config.setJdbcUrl(upserter.urlAssemble(uriSpec.getScheme(), uriSpec.getHost(), uriSpec.getFile()));
		config.setUsername(uriSpec.getUsername());
		config.setPassword(uriSpec.getPassword());
		uriSpec.getParameters().forEach(config::addDataSourceProperty);
		return config;
	}

	@Override
	public void close() {
		HikariDataSource hds = client();
		if (null != hds && hds.isRunning()) hds.close();
	}

	@Override
	public JdbcInput input(String... table) throws IOException {
		// TODO: 2018/5/7 unmatched function
		/*
		 * JdbcInput input = new JdbcInput(); input.table(table); return input;
		 */
		return null;
	}

	@Override
	public JdbcOutput output() throws IOException {
		return new JdbcOutput("JdbcOutput", this);
	}

	private static String buildField(FieldDesc field) {
		StringBuilder sb = new StringBuilder();
		switch (field.type.flag) {
		case INT:
			sb.append(field.name).append(" number(32, 0)");
			break;
		case LONG:
			sb.append(field.name).append(" number(64, 0)");
			break;
		case STR:
			sb.append(field.name).append(" varchar2(100)");
			break;
		case DATE:
			sb.append(field.name).append(" date");
			break;
		default:
			break;
		}
		if (field.rowkey) sb.append(" not null primary key");
		return sb.toString();
	}
}
