package net.butfly.albatis.parquet;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;

import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.AvroFormat;
import net.butfly.alserdes.avro.AvroSerDes.Builder;

class HiveParquetWriter {
	private final HiveParquetOutput out;
	private final Schema avroSchema;
	private final AtomicLong count = new AtomicLong();
	private final long threshold;
	private final Path base;

	private Path current;
	private OutputFile outfile;
	private ParquetWriter<GenericRecord> writer;

	public HiveParquetWriter(HiveParquetOutput out, Qualifier table) {
		super();
		this.out = out;
		TableDesc td = out.schema(table.toString());
		this.threshold = td.attr("hive.parquet.record.threshold", 1000);
		this.avroSchema = AvroFormat.Builder.schema(td);
		this.base = new Path(out.conn.base, table.name);
		rolling();
	}

	public synchronized void write(List<Rmap> l) {
		for (Rmap r : l) {
			try {
				writer.write(Builder.rec(r, avroSchema));
			} catch (IOException e) {
				out.logger().error("Parquet fail on record: \n\t" + r.toString(), e);
			}
			if (count.incrementAndGet() >= threshold) rolling();
		}
	}

	public HiveParquetWriter rolling() {
		if (null != writer) try {
			writer.close();
		} catch (IOException e) {
			out.logger().error("Parquet file close failed.", e);
		}
		current = new Path(base, filename());
		out.logger().info("Parquet file rolled: " + current.toString());
		try {
			outfile = HadoopOutputFile.fromPath(current, out.conn.conf);
			writer = AvroParquetWriter.<GenericRecord> builder(outfile).withSchema(avroSchema).build();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return this;
	}

	private static final SimpleDateFormat f = new SimpleDateFormat(//
			"yyyy" + Path.SEPARATOR_CHAR + "MM" + Path.SEPARATOR_CHAR + "dd" + Path.SEPARATOR_CHAR + "hhmmssSSS");

	private String filename() {
		return f.format(new Date()) + ".parquet";
	}
}
