package net.butfly.albatis.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class JdbcOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = 5114292900867103434L;
	private final JdbcConnection conn;

	/**
	 * constructor with default upsert = true
	 * 
	 * @param name output name
	 * @param conn standard jdbc connection
	 */
	public JdbcOutput(String name, JdbcConnection conn) {
		super(name);
		this.conn = conn;
		closing(this::close);
		open();
	}

	@Override
	public URISpec target() {
		return conn.uri();
	}

	@Override
	protected void enqsafe(Sdream<Rmap> items) {
		AtomicLong n = new AtomicLong(0);
		Map<String, List<Rmap>> mml = items.list().stream().filter(item -> {
			Qualifier table = item.table();
			return null != table && null != table.name && !table.name.isEmpty();
		}).collect(Collectors.groupingBy(r -> r.table().name));
		if (mml.isEmpty()) {
			succeeded(0);
			return;
		}
		try (Connection c = conn.client.getConnection()) {
			n.addAndGet(conn.dialect.upsert(mml, c));
		} catch (SQLException e) {
			logger().error("failed to insert or update items.", e);
		}
		succeeded(n.get());
	}

	@Override
	public void close() {
		conn.close();
	}
}
