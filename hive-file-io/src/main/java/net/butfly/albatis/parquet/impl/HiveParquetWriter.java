package net.butfly.albatis.parquet.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.AvroFormat;
import net.butfly.albatis.parquet.HiveConnection;
import net.butfly.alserdes.avro.AvroSerDes.Builder;

public abstract class HiveParquetWriter extends HiveWriter {
	protected static final Logger logger = Logger.getLogger(HiveParquetWriter.class);
	private static Map<Qualifier, Schema> AVRO_SCHEMAS = Maps.of();

	protected final Schema avroSchema;
	private final Path partitionBase;
	protected Path current;
	protected ParquetWriter<GenericRecord> writer;

	private final ReentrantLock lock = new ReentrantLock();

	public HiveParquetWriter(TableDesc table, HiveConnection conn, Path base) {
		super(table, conn, base);
		// this.threshold = s.rollingRecord;
		// this.timeout = s.rollingMS;
		this.avroSchema = AVRO_SCHEMAS.computeIfAbsent(table.qualifier, q -> AvroFormat.Builder.schema(table));
		this.partitionBase = base;
		// this.logger = Logger.getLogger(HiveParquetWriter.class + "#" + base.toString());
		rolling(true);
	}

	protected abstract ParquetWriter<GenericRecord> createWriter() throws IOException;

	protected abstract long currentBytes();

	@Override
	public void write(List<Rmap> l) {
		while (!lock.tryLock()) try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
			return;
		}
		long now = System.currentTimeMillis();
		long total = 0;
		try {
			for (Rmap r : l) try {
				writer.write(Builder.rec(r, avroSchema));
				lastWriten.set(System.currentTimeMillis());
				total = count.accumulateAndGet(1, (c, one) -> check(c));
			} catch (Exception e) {
				logger.error("Parquet fail on record: \n\t" + r.toString(), e);
			}
		} finally {
			lock.unlock();
		}
		if (logger.isTraceEnabled()) logger.trace("Parquet [" + this.partitionBase.toString() + "] writen [" + l.size() + "] records, "//
				+ "spent [" + (System.currentTimeMillis() - now) + "] ms, total " + total);
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
	@Override
	public HiveParquetWriter rolling(boolean writing) {
		boolean locked;
		if (writing) locked = lock.tryLock();
		else while (!(locked = lock.tryLock())) try {
			Thread.sleep(10);
		} catch (InterruptedException e) {}

		try {
			Path old = current;
			current = new Path(partitionBase, ".current/current.parquet");
			if (null != writer) try {
				writer.close();
			} catch (IOException e) {
				logger.error("Parquet file close failed.", e);
			} finally {
				try {
					String fn = filename();
					conn.client.rename(old, new Path(old.getParent().getParent(), fn));
					logger.info("Parquet file " + partitionBase.toString() + " rolled into: " + fn);
				} catch (IOException e) {
					logger.error("Parquet file rename failed (after being closed): " + old, e);
				}
				// Exeter.of().submit(() -> rename(old));
			}
			if (!writing) { // from refreshing, file timeout, if empty, close self.
				return count.get() <= 0 ? null : this;
			}
			try {
				writer = createWriter();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			return this;
		} finally {
			if (locked) lock.unlock();
		}
	}

	private static final SimpleDateFormat FILENAME_FORMAT = new SimpleDateFormat("hhmmssSSS");

	private synchronized String filename() {
		return FILENAME_FORMAT.format(new Date()) + ".parquet";
	}

	@Override
	public void close() {
		try {
			writer.close();
		} catch (IOException e) {
			logger.error("Parquet writer close failed on: " + current);
		} finally {
			rolling(false);
		}
	}
}
