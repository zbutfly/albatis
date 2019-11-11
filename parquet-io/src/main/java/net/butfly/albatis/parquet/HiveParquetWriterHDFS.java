package net.butfly.albatis.parquet;

import java.io.IOException;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;

import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.io.Rmap;
import net.butfly.alserdes.avro.AvroSerDes.Builder;

class HiveParquetWriterHDFS extends HiveParquetWriter {
	private OutputFile outfile;

	public HiveParquetWriterHDFS(HiveParquetOutput out, Qualifier table) {
		super(out, table);
	}

	@Override
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

	@Override
	protected ParquetWriter<GenericRecord> w() throws IOException {
		outfile = HadoopOutputFile.fromPath(current, out.conn.conf);
		return AvroParquetWriter.<GenericRecord> builder(outfile).withSchema(avroSchema).build();
	}
}
