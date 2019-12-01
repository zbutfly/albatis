package net.butfly.albatis.parquet.impl;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.Path;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.parquet.HiveConnection;

public abstract class HiveWriter implements AutoCloseable {
	public static final String PARTITION_STRATEGY_IMPL_PARAM = "hive.parquet.partition.strategy.impl";
	public static final long WRITER_STATS_STEP = Long.parseLong(Configs.gets("net.butfly.albatis.parquet.writer.stats.step.single", "-1"));
	protected final HiveConnection conn;
	public final TableDesc table;
	public final PartitionStrategy strategy;
	public final AtomicLong lastWriten;
	protected final Path partitionBase;

	private static final long STATS_STEP = Long.parseLong(Configs.gets("net.butfly.albatis.parquet.writer.stats.step", "100000"));
	protected static final Statistic s = new Statistic(HiveWriter.class).step(STATS_STEP);

	public HiveWriter(TableDesc table, HiveConnection conn, Path base) {
		this.conn = conn;
		this.table = table;
		this.strategy = table.attr(PARTITION_STRATEGY_IMPL_PARAM);
		this.lastWriten = new AtomicLong(System.currentTimeMillis());
		this.partitionBase = base;
	}

	public abstract void write(List<Rmap> l);

	@Override
	public abstract void close();

	public abstract HiveWriter rolling(boolean forcing);

	public abstract long currentBytes();

	private static final SimpleDateFormat FILENAME_FORMAT = new SimpleDateFormat("yyyyMMddHHmmssSSS");

	protected String filename() {
		synchronized (FILENAME_FORMAT) {
			return FILENAME_FORMAT.format(new Date()) + "-" + UUID.randomUUID() + ".parquet";
		}
	}
}
