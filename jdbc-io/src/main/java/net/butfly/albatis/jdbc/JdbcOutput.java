package net.butfly.albatis.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.R;
import net.butfly.albatis.io.OutputBase;

public class JdbcOutput extends OutputBase<R> {
	private static final long serialVersionUID = 5114292900867103434L;
	private final JdbcConnection jc;

	/**
	 * constructor with default upsert = true
	 * 
	 * @param name
	 *            output name
	 * @param conn
	 *            standard jdbc connection
	 */
	public JdbcOutput(String name, JdbcConnection conn) {
		super(name);
		this.jc = conn;
		closing(this::close);
		open();
	}

	@Override
	protected void enqueue0(Sdream<R> items) {
		AtomicLong n = new AtomicLong(0);
		Map<String, List<R>> mml = items.list().stream().filter(item -> {
			String table = item.table();
			return null != table && !table.isEmpty();
		}).collect(Collectors.groupingBy(R::table));
		if (mml.isEmpty()) {
			succeeded(0);
			return;
		}
		try (Connection conn = jc.client().getConnection()) {
			n.addAndGet(jc.upserter.upsert(mml, conn));
		} catch (SQLException e) {
			logger().error("failed to insert or update items.");
		}
		succeeded(n.get());
	}

	@Override
	public void close() {
		jc.close();
	}
}
