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
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.AvroFormat;
import net.butfly.albatis.parquet.HiveConnection;
import net.butfly.alserdes.avro.AvroSerDes.Builder;

public abstract class HiveParquetWriter implements AutoCloseable {
	public static final String PARTITION_STRATEGY_IMPL_PARAM = "hive.parquet.partition.strategy.impl";
	public static final String HIVE_JDBC_PARAM = "hive.parquet.jdbc.connect.str";
	private final Logger logger;
	private static Map<Qualifier, Schema> AVRO_SCHEMAS = Maps.of();

	protected final HiveConnection conn;
	public final TableDesc table;
	protected final Schema avroSchema;
	protected Path current;
	protected ParquetWriter<GenericRecord> writer;

	public final AtomicLong lastWriten;
	private final AtomicLong count = new AtomicLong();
	public final AtomicLong lastRefresh = new AtomicLong();
	private final ReentrantLock lock = new ReentrantLock();

	private final Path base;
	public final PartitionStrategy strategy;

	public HiveParquetWriter(TableDesc table, HiveConnection conn, Path base) {
		super();
		this.conn = conn;
		this.table = table;
		this.strategy = table.attr(PARTITION_STRATEGY_IMPL_PARAM);
		// this.threshold = s.rollingRecord;
		// this.timeout = s.rollingMS;
		this.avroSchema = AVRO_SCHEMAS.computeIfAbsent(table.qualifier, q -> AvroFormat.Builder.schema(table));
		this.base = base;
		this.logger = Logger.getLogger(HiveParquetWriter.class + "#" + base.toString());
		this.lastWriten = new AtomicLong(System.currentTimeMillis());
		rolling(true);
	}

	protected abstract ParquetWriter<GenericRecord> w() throws IOException;

	protected abstract long currentBytes();

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
		if (strategy.rollingRecord <= 0 || c < strategy.rollingRecord) return c;
		if (currentBytes() > strategy.rollingByte) return c;
		rolling(true);
		return 0;
	}

	/**
	 * @param internal:
	 *            rolling by writing, if not, rolling by monitor.
	 * @return
	 */
	public HiveParquetWriter rolling(boolean writing) {
		boolean locked;
		if (writing) locked = lock.tryLock();
		else while (!(locked = lock.tryLock())) try {
			Thread.sleep(10);
		} catch (InterruptedException e) {}

		try {
			Path old = current;
			current = new Path(base, filename());
			if (null != writer) try {
				writer.close();
			} catch (IOException e) {
				logger.error("Parquet file close failed.", e);
			} finally {
				Exeter.of().submit(() -> rename(old));
			}
			if (!writing) { // from refreshing, file timeout, if empty, close self.
				return count.get() <= 0 ? null : this;
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

	private void rename(Path old) {
		String fn = old.getName();
		fn = fn.substring(0, fn.lastIndexOf('.'));
		try {
			conn.fs.rename(old, new Path(old.getParent(), fn));
		} catch (IOException e) {
			logger.error("Parquet file rename failed (after being closed): " + old, e);
		}
	}

	private static final SimpleDateFormat FILENAME_FORMAT = new SimpleDateFormat("hhmmssSSS");

	private String filename() {
		return FILENAME_FORMAT.format(new Date()) + ".parquet.current";
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
