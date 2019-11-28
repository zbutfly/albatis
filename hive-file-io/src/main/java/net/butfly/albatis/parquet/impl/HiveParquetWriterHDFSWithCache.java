package net.butfly.albatis.parquet.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

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
	private static Map<Qualifier, Schema> AVRO_SCHEMAS = Maps.of();

	protected final Schema avroSchema;
	protected final BlockingQueue<Rmap> pool = new LinkedBlockingQueue<>();

	public HiveParquetWriterHDFSWithCache(TableDesc table, HiveConnection conn, Path base) {
		super(table, conn, base);
		this.avroSchema = AVRO_SCHEMAS.computeIfAbsent(table.qualifier, q -> AvroFormat.Builder.schema(table));
	}

	@Override
	public void write(List<Rmap> l) {
		s.statsOuts(l, pool::addAll);
		rolling(true); // s.statsOutN
	}

	/**
	 * @param internal:
	 *            rolling by writing, if not, rolling by monitor.
	 * @return
	 */
	@Override
	public HiveParquetWriterHDFSWithCache rolling(boolean unforce) {
		List<Rmap> l = drain(!unforce);
		if (l.isEmpty()) return this;
		Path p = new Path(partitionBase, filename());
		logger.debug("Parquet writing and putting begin in new thread on " + p);
		new Thread(() -> upload(p, l), "ParquetWriting@" + p).start();
		return this;
	}

	private synchronized List<Rmap> drain(boolean force) {
		List<Rmap> l = Colls.list();
		if (force) pool.drainTo(l);
		else if (pool.size() >= strategy.rollingRecord) pool.drainTo(l, strategy.rollingRecord);
		return l;
	}

	private void upload(Path p, List<Rmap> l) {
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
			lastWriten.set(System.currentTimeMillis());
			logger.debug("Parquet " + p + " put finished in " + (System.currentTimeMillis() - now) + " ms, "//
					+ "with " + l.size() + " records and " + bytes + " bytes.");
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
		rolling(false);
	}

	@Override
	protected long currentBytes() {
		return -1;
	}
}
