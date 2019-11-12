package net.butfly.albatis.parquet.impl;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;

import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.parquet.HiveParquetOutput;

public class HiveParquetWriterHDFS extends HiveParquetWriter {
	private OutputFile outfile;

	public HiveParquetWriterHDFS(HiveParquetOutput out, Qualifier table, Configuration conf, Path base) {
		super(out, table, conf, base);
	}

	@Override
	protected ParquetWriter<GenericRecord> w() throws IOException {
		outfile = HadoopOutputFile.fromPath(current, conf);
		return AvroParquetWriter.<GenericRecord> builder(outfile).withSchema(avroSchema).build();
	}
}
