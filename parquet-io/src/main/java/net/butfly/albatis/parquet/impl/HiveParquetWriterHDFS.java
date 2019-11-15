package net.butfly.albatis.parquet.impl;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;

import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.parquet.HiveConnection;

public class HiveParquetWriterHDFS extends HiveParquetWriter {
	private OutputFile outfile;

	public HiveParquetWriterHDFS(TableDesc table, HiveConnection conn, Path base) {
		super(table, conn, base);
	}

	@Override
	protected ParquetWriter<GenericRecord> createWriter() throws IOException {
		outfile = HadoopOutputFile.fromPath(current, conn.conf);
		return AvroParquetWriter.<GenericRecord> builder(outfile).withSchema(avroSchema).build();
	}

	@Override
	protected long currentBytes() {
		return writer.getDataSize();
	}
}
