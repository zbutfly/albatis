package net.butfly.albatis.parquet;

import static org.apache.avro.Schema.create;
import static org.apache.avro.Schema.createRecord;
import static org.apache.avro.Schema.createUnion;
import static net.butfly.albatis.parquet.utils.Hdfses.*;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;

public class ParquetTest {
	private static final java.nio.file.Path dstFile = java.nio.file.Path.of("C:\\Temp\\test.parquet");
	private static final Path dst = new Path(dstFile.toUri());
	private static final String hdfs = "hdfs://127.0.0.1:11000/";

	private static final Schema schema = createRecord("TestNap", null, null, false);
	static {
		schema.setFields(Arrays.asList(//
				new Field("NAME", createUnion(Arrays.asList(create(Type.STRING), create(Type.NULL))), null, ""), //
				new Field("AGE", createUnion(Arrays.asList(create(Type.INT), create(Type.NULL))), null, null)//
		));
	}
	private static final Random rd = new Random(); // creating Random object

	public static void main(String[] args) throws IOException {
		// write();
		// read();
		basic();
	}

	static void read() throws IOException {
		try (ParquetReader<GenericRecord> r = AvroParquetReader.<GenericRecord> builder(dst).build();) {
			GenericRecord rr;
			while (null != (rr = r.read())) System.err.println(rr);
		}
	}

	static void write() throws IOException {
		File f = dstFile.toFile();
		if (f.exists() && f.isFile()) f.delete();

		GenericRecord rec = new GenericData.Record(schema);

		try (ParquetWriter<GenericRecord> w = AvroParquetWriter.<GenericRecord> builder(dst).withSchema(rec.getSchema()).build();) {
			for (int i = 0; i < 100; i++) {
				int age = rd.nextInt(100) + 20;
				rec.put("NAME", "zx-" + age);
				rec.put("AGE", age);
				w.write(rec);
			}
		}
	}

	static void basic() throws IOException {
		Configuration conf = manualHadoopConfiguration(hdfs);

		FileSystem fs = FileSystem.get(conf);
		// ls
		RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path("/"), true);
		while (it.hasNext()) {
			LocatedFileStatus f = it.next();
			System.out.println(f.toString());
		}
		// cat
		try (FSDataInputStream f = fs.open(new Path("/init.cmd"));) {
			String out = org.apache.commons.io.IOUtils.toString(f, "UTF-8");
			System.out.println(out);
		}

	}
}
