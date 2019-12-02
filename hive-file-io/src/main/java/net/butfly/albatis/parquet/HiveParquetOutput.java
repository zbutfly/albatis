package net.butfly.albatis.parquet;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.Path;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.StatsUtils;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.parquet.impl.HiveParquetWriterHDFS;
import net.butfly.albatis.parquet.impl.HiveParquetWriterHDFSWithCacheMM;
import net.butfly.albatis.parquet.impl.HiveParquetWriterLocal;
import net.butfly.albatis.parquet.impl.HiveWriter;
import net.butfly.albatis.parquet.impl.PartitionStrategy;
import net.butfly.albatis.parquet.utils.BigBlockingQueue;

public class HiveParquetOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = 4543231903669455241L;

	final HiveConnection conn;
	private Map<Path, HiveWriter> writers = Maps.of();
	private final Thread monitor;
	private final Map<TableDesc, Long> nextRefreshs = Maps.of();
	private final AtomicLong total = new AtomicLong();

	public HiveParquetOutput(String name, HiveConnection conn, TableDesc... table) throws IOException {
		super(name);
		this.conn = conn;
		monitor = new Thread(() -> check(), "HiveParquetFileWriterMonitor");
		monitor.setDaemon(true);
		monitor.start();
		long now = System.currentTimeMillis();
		for (TableDesc td : table) nextRefreshs.computeIfAbsent(td, t -> now + ((PartitionStrategy) td.attr(
				HiveWriter.PARTITION_STRATEGY_IMPL_PARAM)).refreshMS());
	}

	private HiveWriter w(Qualifier table, String subdir) {
		Path path = new Path(conn.base, table.name);
		if (null != subdir && !subdir.isEmpty()) path = new Path(path, subdir);
		TableDesc td = schema(table);
		return writers.computeIfAbsent(path, p -> {
			if (null == conn.conf) {
				logger().trace("Parquet local writer created for " + td.qualifier + " on " + p);
				return new HiveParquetWriterLocal(td, conn, p);
			} else if (null == BigBlockingQueue.BASE) {
				logger().trace("Parquet hdfs writer created for " + td.qualifier + " on " + p + //
				", with hdfs " + conn.conf.get("fs.default.name") + " [" + conn.conf.toString() + "]");
				return new HiveParquetWriterHDFS(td, conn, p);
			} else {
				logger().trace("Parquet hdfs writer created for " + td.qualifier + " on " + p + ", with cache: " + BigBlockingQueue.BASE);
				return new HiveParquetWriterHDFSWithCacheMM(td, conn, p);
			}
		});
	}

	@Override
	protected void enqsafe(Sdream<Rmap> items) {
		Map<Qualifier, Map<String, List<Rmap>>> split = Maps.of();
		items.eachs(r -> {
			total.incrementAndGet();
			split.computeIfAbsent(r.table(), t -> Maps.of())//
					.computeIfAbsent(partition(r, schema(r.table())), h -> Colls.list()).add(r);
		});
		split.forEach((t, m) -> m.forEach((h, l) -> w(t, h).write(l)));
		if (HiveWriter.WRITER_STATS_STEP > 0 && logger().isDebugEnabled() && total.get() % HiveWriter.WRITER_STATS_STEP == 0) {
			AtomicLong bytes = new AtomicLong();
			writers.values().forEach(w -> bytes.addAndGet(w.currentBytes()));
			logger().debug("Parquet output " + total.get() + " records with " + StatsUtils.formatKilo(bytes.get(), "Bytes") + ", current "
					+ writers.size() + " writers.");
		}
	}

	private String partition(Rmap r, TableDesc td) {
		PartitionStrategy s = td.attr(HiveWriter.PARTITION_STRATEGY_IMPL_PARAM);
		return null == s ? "" : s.partition(r);
	}

	@Override
	public void close() {
		super.close();
		Path p;
		HiveWriter w;
		Set<TableDesc> tbls = new HashSet<>();
		try {
			while (true) {
				logger().info("Parquet output closing, waiting for " + writers.size() + " writers...");
				try {
					p = writers.keySet().iterator().next();
				} catch (NoSuchElementException e) {
					return;
				} ;
				if (null != (w = writers.remove(p))) {
					tbls.add(w.table);
					w.close();
				}
			}
		} finally {
			if (!tbls.isEmpty()) {
				logger().info("Parquet output closing, refreshing " + tbls.size() + " tables...");
				refresh(tbls.toArray(new TableDesc[0]));
			}
		}
	}

	private void check() {
		while (true) {
			Set<TableDesc> toRefresh = new HashSet<>();
			long now = System.currentTimeMillis();
			for (Path p : writers.keySet()) {
				writers.compute(p, (pp, w) -> {
					HiveWriter ww = w.strategy.rollingMS() < System.currentTimeMillis() - w.lastWriten.get() ? w.rolling(true) : w;
					if (now > nextRefreshs.get(w.table)) toRefresh.add(w.table);
					return ww;
				});
			}
			refresh(toRefresh.toArray(new TableDesc[0]));
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {}
		}
	}

	private synchronized void refresh(TableDesc... tables) {
		if (tables.length == 0) return;
		long now = System.currentTimeMillis();
		try {
			String sql = "msck repair table ";
			for (TableDesc t : tables) {
				PartitionStrategy s = t.attr(HiveWriter.PARTITION_STRATEGY_IMPL_PARAM);
				String jdbc = s.jdbcUri;
				logger().info("Refresh hive table " + t.qualifier + " on: " + jdbc);
				try (Connection c = DriverManager.getConnection(jdbc)) {
					try (Statement ps = c.createStatement();) {
						ps.execute(sql + t.qualifier.toString());
					} catch (SQLException e) {
						logger().error("Refresh failed on table: " + t, e);
					}
				} catch (SQLException e) {
					logger().error("Refresh connection failed on jdbc: " + jdbc, e);
				} finally {
					nextRefreshs.put(t, System.currentTimeMillis() + s.refreshMS());
				}
			}
		} finally {
			logger().info("Refreshing ended in " + (System.currentTimeMillis() - now) + " ms.");
		}
	}
}
