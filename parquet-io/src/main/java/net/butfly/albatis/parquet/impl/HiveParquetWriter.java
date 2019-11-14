package net.butfly.albatis.parquet.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.AvroFormat;
import net.butfly.alserdes.avro.AvroSerDes.Builder;

public abstract class HiveParquetWriter implements AutoCloseable {
	private static Map<Qualifier, Schema> AVRO_SCHEMAS = Maps.of();

	// public static final String ROLLING_RECORD_COUNT_PARAM = "hive.parquet.rolling.records";
	// public static final String ROLLING_DURATION_SEC_PARAM = "hive.parquet.rolling.seconds";
	// public static final String PARTITION_STRATEGY_DESC_PARAM = "hive.parquet.partition.strategy";
	public static final String PARTITION_STRATEGY_IMPL_PARAM = "hive.parquet.partition.strategy.impl";
	// public static final String PARTITION_FIELD_NAME_PARAM = "hive.parquet.partition.field";
	// public static final String PARTITION_FIELD_PARSE_FORMAT_PARAM = "yyyy-MM-dd hh:mm:ss";

	public final AtomicLong lastWriten;
	public final long timeout;

	private final AtomicLong count = new AtomicLong();
	private final long threshold;
	private final Path base;
	private final Logger logger;
	private final ReentrantLock lock = new ReentrantLock();

	protected final Configuration conf;
	protected final Schema avroSchema;
	protected Path current;
	protected ParquetWriter<GenericRecord> writer;

	public HiveParquetWriter(TableDesc table, Configuration conf, Path base, Logger logger) {
		super();
		this.conf = conf;
		PartitionStrategy s = table.attr(PARTITION_STRATEGY_IMPL_PARAM);
		this.threshold = s.rollingRecord;
		this.timeout = s.rollingMS;
		this.avroSchema = AVRO_SCHEMAS.computeIfAbsent(table.qualifier, q -> AvroFormat.Builder.schema(table));
		this.base = base;
		this.logger = logger;
		this.lastWriten = new AtomicLong(System.currentTimeMillis());
		rolling();
	}

	public void write(List<Rmap> l) {
		while (!lock.tryLock()) try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
			return;
		}
		try {
			for (Rmap r : l) try {
				writer.write(Builder.rec(r, avroSchema));
				lastWriten.set(System.currentTimeMillis());
				count.accumulateAndGet(1, (c, one) -> check(c));
			} catch (IOException e) {
				logger.error("Parquet fail on record: \n\t" + r.toString(), e);
			}
		} finally {
			lock.unlock();
		}
	}

	private long check(long c) {
		c++;
		if (threshold <= 0 || c < threshold) return c;
		rolling();
		return 0;
	}

	public HiveParquetWriter rolling() {
		boolean locked = lock.tryLock();
		try {
			current = new Path(base, filename());
			if (null != writer) try {
				writer.close();
			} catch (IOException e) {
				logger.error("Parquet file close failed.", e);
			}
			try {
				writer = w();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			logger.info("Parquet file rolled into: " + current.toString());
			return this;
		} finally {
			if (locked) lock.unlock();
		}
	}

	protected abstract ParquetWriter<GenericRecord> w() throws IOException;

	private static final SimpleDateFormat FILENAME_FORMAT = new SimpleDateFormat("hhmmssSSS");

	private String filename() {
		return FILENAME_FORMAT.format(new Date()) + ".parquet";
	}

	@Override
	public void close() {
		try {
			writer.close();
		} catch (IOException e) {
			logger.error("Parquet writer close failed on: " + current);
		}
	}
}
