package net.butfly.albatis.parquet.impl;

import java.io.File;
import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.parquet.HiveParquetOutput;

public class HiveParquetWriterLocal extends HiveParquetWriter {
	public HiveParquetWriterLocal(HiveParquetOutput out, Qualifier table, Configuration conf, Path base) {
		super(out, table, conf, base);
	}

	@Override
	protected ParquetWriter<GenericRecord> w() throws IOException {
		File local = java.nio.file.Path.of(current.toString()).toFile();
		if (!local.getParentFile().exists() && !local.getParentFile().mkdirs()) //
			throw new IOException("Parents dirs create fail on: " + local);
		// if ((!local.exists() || local.isDirectory()) && !local.createNewFile())// local parquet write need non-existed file
		// throw new IOException("File create fail on: " + local);
		return AvroParquetWriter.<GenericRecord> builder(current).withSchema(avroSchema).build();
	}
}
