package net.butfly.albatis.parquet.impl;

import java.io.IOException;
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
import net.butfly.albacore.utils.logger.StatsUtils;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.AvroFormat;
import net.butfly.albatis.parquet.HiveConnection;
import net.butfly.alserdes.avro.AvroSerDes.Builder;

public abstract class HiveParquetWriter extends HiveWriter {
	protected static final Logger logger = Logger.getLogger(HiveParquetWriter.class);
	private static Map<Qualifier, Schema> AVRO_SCHEMAS = Maps.of();

	public final AtomicLong count = new AtomicLong();
	protected final Schema avroSchema;
	protected ParquetWriter<GenericRecord> writer;
	protected Path current;

	private final ReentrantLock lock = new ReentrantLock();

	public HiveParquetWriter(TableDesc table, HiveConnection conn, Path base) {
		super(table, conn, base);
		// s.detailing(() -> "current [" + current.toString() + "] writed: " + writer.getDataSize());
		this.avroSchema = AVRO_SCHEMAS.computeIfAbsent(table.qualifier, q -> AvroFormat.Builder.schema(table));
		rolling(false);
	}

	protected abstract ParquetWriter<GenericRecord> createWriter() throws IOException;

	@Override
	public void write(List<Rmap> l) {
		while (!lock.tryLock()) try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
			return;
		}
		try {
			for (Rmap r : l) s.statsOutN(r, this::write0);
		} finally {
			lock.unlock();
		}
	}

	private long write0(Rmap rr) {
		long b = writer.getDataSize();
		try {
			writer.write(Builder.rec(rr, avroSchema));
			lastWriten.set(System.currentTimeMillis());
			long cc = count.accumulateAndGet(1, (c, one) -> check(c));
			if (WRITER_STATS_STEP > 0 && cc % WRITER_STATS_STEP == 0) //
				logger.trace(cc + " record writen, size " + writer.getDataSize() + " bytes on " + current.toString());
		} catch (IOException e) {
			logger.error("Parquet fail on record: \n\t" + rr.toString(), e);
		}
		return writer.getDataSize() - b;
	}

	private long check(long c) {
		c++;
		if (strategy.rollingRecord <= 0 || c < strategy.rollingRecord) return c;
		if (currentBytes() > strategy.rollingByte) return c;
		rolling(false);
		return 0;
	}

	/**
	 * @param internal:
	 *            rolling by writing, if not, rolling by monitor.
	 * @return
	 */
	@Override
	public HiveParquetWriter rolling(boolean forcing) {
		boolean locked;
		if (!forcing) locked = lock.tryLock();
		else while (!(locked = lock.tryLock())) try {
			Thread.sleep(10);
		} catch (InterruptedException e) {}
		try {
			Path old = current;
			current = new Path(new Path(partitionBase, ".current"), filename());
			Exeter.of().submit(() -> {
				long now = System.currentTimeMillis();
				if (null != writer) {
					String bytes = StatsUtils.formatKilo(writer.getDataSize(), "bytes");
					long c = count.get();
					try {
						writer.close();
					} catch (IOException e) {
						logger.error("Parquet file close failed.", e);
					} finally {
						try {
							if (!conn.client.rename(old, new Path(old.getParent().getParent(), old.getName()))) //
								logger.error("Parquet file closed but move failed: " + old.toString());
						} catch (IOException e) {
							logger.error("Parquet file closed but move failed: " + old.toString(), e);
						} finally {
							logger.trace("Parquet file rolling with " + bytes + "/" + c + " records finished, spent " //
									+ (System.currentTimeMillis() - now) + " ms: " + old.toString());
						}
					}
				}
			});
			if (forcing) // from refreshing, file timeout, if empty, close self.
				return count.get() <= 0 ? null : this;
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

	@Override
	public void close() {
		try {
			writer.close();
		} catch (IOException e) {
			logger.error("Parquet writer close failed on: " + current);
		} finally {
			rolling(true);
		}
	}

	@Override
	public long currentBytes() {
		return writer.getDataSize();
	}
}
