package net.butfly.albatis.parquet.impl;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import com.opencsv.CSVWriter;

import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.parquet.HiveConnection;

public class HiveCsvWrite extends HiveWriter {
	protected CSVWriter writer;

	public HiveCsvWrite(TableDesc table, HiveConnection conn, Path base) {
		super(table, conn, base);
	}

	@Override
	public void write(List<Rmap> l) {
		// TODO Auto-generated method stub
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public HiveParquetWriter rolling(boolean writing) {
		// TODO Auto-generated method stub
		return null;
	}

	protected CSVWriter createWriter() throws IOException {
		FSDataOutputStream os = conn.client.create(current);
		return new CSVWriter(new OutputStreamWriter(os));
	}
}
