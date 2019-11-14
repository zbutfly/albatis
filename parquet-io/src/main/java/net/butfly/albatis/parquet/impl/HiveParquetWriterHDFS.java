package net.butfly.albatis.parquet.impl;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;

import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.TableDesc;

public class HiveParquetWriterHDFS extends HiveParquetWriter {
	private OutputFile outfile;

	public HiveParquetWriterHDFS(TableDesc table, Configuration conf, Path base, Logger logger) {
		super(table, conf, base, logger);
	}

	@Override
	protected ParquetWriter<GenericRecord> w() throws IOException {
		outfile = HadoopOutputFile.fromPath(current, conf);
		return AvroParquetWriter.<GenericRecord> builder(outfile).withSchema(avroSchema).build();
	}

	@Override
	protected long currentBytes() {
		return writer.getDataSize();
	}
}
