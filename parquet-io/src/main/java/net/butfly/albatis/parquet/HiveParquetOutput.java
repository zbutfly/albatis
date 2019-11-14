package net.butfly.albatis.parquet;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.Path;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.parquet.impl.HiveParquetWriter;
import net.butfly.albatis.parquet.impl.HiveParquetWriterHDFS;
import net.butfly.albatis.parquet.impl.HiveParquetWriterLocal;
import net.butfly.albatis.parquet.impl.PartitionStrategy;

public class HiveParquetOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = 4543231903669455241L;

	final HiveConnection conn;
	private Map<Path, HiveParquetWriter> writers = Maps.of();
	private final Thread monitor;

	public HiveParquetOutput(String name, HiveConnection conn, TableDesc... table) throws IOException {
		super(name);
		this.conn = conn;
		// for (TableDesc t : table) {
		// PartitionStrategy s = PartitionStrategy.strategy(t.attr(HiveParquetWriter.PARTITION_STRATEGY_DESC_PARAM));
		// if (null != s) t.attw(HiveParquetWriter.PARTITION_STRATEGY_IMPL_PARAM, s);
		// // w(t);// rolling initialization tables.
		// }
		monitor = new Thread(() -> check(), "HiveParquetFileWriterMonitor");
		monitor.setDaemon(true);
		monitor.start();
	}

	private HiveParquetWriter w(Qualifier table, String subdir) {
		Path path = new Path(conn.base, table.name);
		if (null != subdir && !subdir.isEmpty()) path = new Path(path, subdir);
		TableDesc td = schema(table);
		return writers.computeIfAbsent(path, p -> null == conn.conf ? //
				new HiveParquetWriterLocal(td, conn, p) : //
				new HiveParquetWriterHDFS(td, conn, p));
	}

	@Override
	protected void enqsafe(Sdream<Rmap> items) {
		Map<Qualifier, Map<String, List<Rmap>>> split = Maps.of();
		items.eachs(r -> split.computeIfAbsent(r.table(), t -> Maps.of())//
				.computeIfAbsent(partition(r, schema(r.table())), h -> Colls.list()).add(r));
		split.forEach((t, m) -> m.forEach((h, l) -> w(t, h).write(l)));
	}

	private String partition(Rmap r, TableDesc td) {
		PartitionStrategy s = td.attr(HiveParquetWriter.PARTITION_STRATEGY_IMPL_PARAM);
		return null == s ? "" : s.partition(r);
	}

	@Override
	public void close() {
		super.close();
		Path p;
		HiveParquetWriter w;
		while (true) {
			try {
				p = writers.keySet().iterator().next();
			} catch (NoSuchElementException e) {
				return;
			} ;
			if (null != (w = writers.remove(p))) w.close();
		}
	}

	private void check() {
		long now = System.currentTimeMillis();
		while (true) {
			for (Path p : writers.keySet()) {
				writers.compute(p, (pp, w) -> {
					if (w.strategy.rollingMS() > now - w.lastWriten.get()) return w.rolling(false);
					if (w.strategy.refreshMS() > now - w.lastRefresh.get()) refresh();
					return w;
				});
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {}
		}
	}

	private void refresh() {
		long now = System.currentTimeMillis();
		logger().info("Refreshing begin.");
		try {
			String sql = "msck repair table ";
			Map<String, List<String>> dbs = Maps.of();
			writers.forEach((p, w) -> {
				String jdbc = w.table.attr(HiveParquetWriter.HIVE_JDBC_PARAM);
				if (null == jdbc) return;
				dbs.computeIfAbsent(jdbc, j -> Colls.list()).add(w.table.toString());
			});
			dbs.forEach((jdbc, tables) -> {
				try (Connection c = DriverManager.getConnection(jdbc)) {
					tables.forEach(t -> {
						try (Statement ps = c.createStatement();) {
							ps.execute(sql + t);
						} catch (SQLException e) {
							logger().error("Refresh failed on table: " + t, e);
						}
					});
				} catch (SQLException e) {
					logger().error("Refresh connection failed on jdbc: " + jdbc, e);
				}
			});
		} finally {
			logger().info("Refreshing ended in " + (System.currentTimeMillis() - now) + " ms.");

		}
	}
}
