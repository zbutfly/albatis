package net.butfly.albatis.parquet.impl;

import java.io.File;
import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.TableDesc;

public class HiveParquetWriterLocal extends HiveParquetWriter {
	public HiveParquetWriterLocal(TableDesc table, Configuration conf, Path base, Logger logger) {
		super(table, conf, base, logger);
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

	@Override
	protected long currentBytes() {
		return java.nio.file.Path.of(current.toString()).toFile().length();
	}
}
