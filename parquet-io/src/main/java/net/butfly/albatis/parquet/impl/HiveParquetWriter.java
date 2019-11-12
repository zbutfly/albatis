package net.butfly.albatis.parquet.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.AvroFormat;
import net.butfly.albatis.parquet.HiveParquetOutput;
import net.butfly.alserdes.avro.AvroSerDes.Builder;

public abstract class HiveParquetWriter {
	public static final String ROLLING_RECORD_COUNT_PARAM = "hive.parquet.rolling.records";
	public static final String ROLLING_DURATION_SEC_PARAM = "hive.parquet.rolling.seconds";
	public static final String HASHING_STRATEGY_DESC_PARAM = "hive.parquet.hashing.strategy";
	public static final String HASHING_STRATEGY_IMPL_PARAM = "hive.parquet.hashing.strategy.impl";
	public static final String HASHING_FIELD_NAME_PARAM = "hive.parquet.hashing.field";
	public static final String HASHING_FIELD_PARSE_FORMAT_PARAM = "yyyy-MM-dd hh:mm:ss";

	protected final Configuration conf;
	protected final Schema avroSchema;
	private final HiveParquetOutput out;
	private final AtomicLong count = new AtomicLong();
	private final long threshold;
	private final Path base;

	protected Path current;
	protected ParquetWriter<GenericRecord> writer;

	public HiveParquetWriter(HiveParquetOutput out, Qualifier table, Configuration conf, Path base) {
		super();
		this.out = out;
		this.conf = conf;
		TableDesc td = out.schema(table);
		this.threshold = td.attr(ROLLING_RECORD_COUNT_PARAM, 1000);
		this.avroSchema = AvroFormat.Builder.schema(td);
		this.base = new Path(base, table.name);
		rolling();
	}

	public synchronized void write(List<Rmap> l) {
		for (Rmap r : l) {
			try {
				writer.write(Builder.rec(r, avroSchema));
				count.accumulateAndGet(1, (c, one) -> check(c));
			} catch (IOException e) {
				out.logger().error("Parquet fail on record: \n\t" + r.toString(), e);
			}
		}
	}

	private long check(long c) {
		if (++c < threshold) return c;
		rolling();
		return 0;
	}

	protected HiveParquetWriter rolling() {
		current = new Path(base, filename());
		if (null != writer) try {
			writer.close();
		} catch (IOException e) {
			out.logger().error("Parquet file close failed.", e);
		}
		try {
			writer = w();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		out.logger().info("Parquet file rolled into: " + current.toString());
		return this;
	}

	protected abstract ParquetWriter<GenericRecord> w() throws IOException;

	private static final SimpleDateFormat FILENAME_FORMAT = new SimpleDateFormat("hhmmssSSS");

	private String filename() {
		return FILENAME_FORMAT.format(new Date()) + ".parquet";
	}
}
