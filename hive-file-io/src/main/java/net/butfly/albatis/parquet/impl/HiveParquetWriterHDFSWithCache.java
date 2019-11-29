package net.butfly.albatis.parquet.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.AvroFormat;
import net.butfly.albatis.parquet.HiveConnection;
import net.butfly.alserdes.avro.AvroSerDes.Builder;

public class HiveParquetWriterHDFSWithCache extends HiveWriter {
	protected static final Logger logger = Logger.getLogger(HiveParquetWriterHDFSWithCache.class);
	private static final int ROLLING_CONCURRENCY = Integer.parseInt(Configs.gets("net.butfly.albatis.parquet.hdfs.upload.max.concurrency",
			"-1"));
	private static final AtomicInteger uploadsPending = new AtomicInteger();
	private static Map<Qualifier, Schema> AVRO_SCHEMAS = Maps.of();

	protected final Schema avroSchema;
	private final BlockingQueue<Rmap> pool = new LinkedBlockingQueue<>();

	public HiveParquetWriterHDFSWithCache(TableDesc table, HiveConnection conn, Path base) {
		super(table, conn, base);
		this.avroSchema = AVRO_SCHEMAS.computeIfAbsent(table.qualifier, q -> AvroFormat.Builder.schema(table));
	}

	@Override
	public void write(List<Rmap> l) {
		try {
			s.statsOuts(l, pool()::addAll);
		} finally {
			lastWriten.set(System.currentTimeMillis());
		}
		rolling(false);
	}

	/**
	 * @param internal:
	 *            rolling by writing, if not, rolling by monitor.
	 * @return
	 */
	@Override
	public HiveParquetWriterHDFSWithCache rolling(boolean forcing) {
		List<Rmap> l = drain(forcing);
		if (l.isEmpty()) return this;
		Path p = new Path(partitionBase, filename());
		logger.debug("Parquet writing and putting begin in new thread on " + p);
		new Thread(() -> upload(p, l), "ParquetWriting@" + p).start();
		return this;
	}

	private synchronized List<Rmap> drain(boolean force) {
		List<Rmap> l = Colls.list();
		if (force) pool().drainTo(l);
		else if (pool().size() >= strategy.rollingRecord) pool().drainTo(l, strategy.rollingRecord);
		return l;
	}

	private void upload(Path p, List<Rmap> l) {
		if (ROLLING_CONCURRENCY > 0) {
			int c;
			while (ROLLING_CONCURRENCY < (c = uploadsPending.get())) {
				logger.trace("Parquet uploading pending " + c + " exceeds threshold " + ROLLING_CONCURRENCY
						+ " (configurated by net.butfly.albatis.parquet.hdfs.upload.max.concurrency).");
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					break;
				}
			}
		}
		uploadsPending.incrementAndGet();
		try {
			HadoopOutputFile of = open(p);
			long now = System.currentTimeMillis();
			long bytes = 0;
			try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord> builder(of)//
					.withRowGroupSize(10 * 1024 * 1024)//
					.withSchema(avroSchema).build();) {
				for (Rmap rr : l) try {
					writer.write(Builder.rec(rr, avroSchema));
				} catch (IOException e) {
					logger.error("Parquet fail on " + of.toString() + " record: \n\t" + rr.toString(), e);
				}
				bytes = writer.getDataSize();
			} catch (IOException e) {
				logger.error("Parquet fail on writer create: " + of.toString(), e);
			} finally {
				logger.debug("Parquet " + p + " put finished in " + (System.currentTimeMillis() - now) + " ms, "//
						+ "with " + l.size() + " records and " + bytes + " bytes.");
			}
		} finally {
			uploadsPending.decrementAndGet();
		}
	}

	private HadoopOutputFile open(Path p) {
		HadoopOutputFile of;
		try {
			of = HadoopOutputFile.fromPath(p, conn.conf);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		FileStatus f;
		try {
			if (conn.client.exists(p) && (f = conn.client.getFileStatus(p)).isFile()) {
				if (f.getLen() > 0) logger.warn("Parquet working file " + p.toString()
						+ " existed and not zero, clean and recreate it maybe cause data losing.");
				conn.client.delete(p, true);
			}
		} catch (IOException e) {
			logger.error("Working parquet file " + p.toString() + " cleaning fail.", e);
		}
		return of;
	}

	private static final SimpleDateFormat FILENAME_FORMAT = new SimpleDateFormat("yyyyMMddHHmmssSSS");

	private synchronized String filename() {
		return FILENAME_FORMAT.format(new Date()) + ".parquet";
	}

	@Override
	public void close() {
		rolling(true);
	}

	@Override
	public long currentBytes() {
		return -1;
	}

	protected BlockingQueue<Rmap> pool() {
		return pool;
	}
}
