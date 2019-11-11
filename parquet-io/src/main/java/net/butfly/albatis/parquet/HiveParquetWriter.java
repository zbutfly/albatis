package net.butfly.albatis.parquet;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.AvroFormat;

abstract class HiveParquetWriter {
	protected final HiveParquetOutput out;
	protected final Schema avroSchema;
	protected final AtomicLong count = new AtomicLong();
	protected final long threshold;
	private final Path base;

	protected Path current;
	protected ParquetWriter<GenericRecord> writer;

	public HiveParquetWriter(HiveParquetOutput out, Qualifier table) {
		super();
		this.out = out;
		TableDesc td = out.schema(table.toString());
		this.threshold = td.attr("hive.parquet.record.threshold", 1000);
		this.avroSchema = AvroFormat.Builder.schema(td);
		this.base = new Path(out.conn.base, table.name);
		rolling();
	}

	abstract void write(List<Rmap> l);

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
		out.logger().info("Parquet file rolled: " + current.toString());
		return this;
	}

	protected abstract ParquetWriter<GenericRecord> w() throws IOException;

	private static final SimpleDateFormat f = new SimpleDateFormat(//
			"yyyy" + Path.SEPARATOR_CHAR + "MM" + Path.SEPARATOR_CHAR + "dd" + Path.SEPARATOR_CHAR + "hhmmssSSS");

	private String filename() {
		return f.format(new Date()) + ".parquet";
	}
}
