package net.butfly.albatis.parquet.impl;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import com.opencsv.CSVWriter;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.parquet.HiveConnection;

public class HiveCsvWrite extends HiveWriter {
	protected CSVWriter writer;
	protected Path current;

	public HiveCsvWrite(TableDesc table, HiveConnection conn, Path base) {
		super(table, conn, base);
	}

	@Override
	public void write(List<Rmap> l) {
		List<String[]> lines = Colls.list();
		for (Rmap r : l) lines.add(tolines(r));
		if (!lines.isEmpty()) writer.writeAll(lines);
	}

	private String[] tolines(Rmap r) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public HiveParquetWriter rolling(boolean forcing) {
		// TODO Auto-generated method stub
		return null;
	}

	protected CSVWriter createWriter() throws IOException {
		FSDataOutputStream os = conn.client.create(current);
		CSVWriter w = new CSVWriter(new OutputStreamWriter(os));
		return w;
	}

	@Override
	public long currentBytes() {
		return 0;
	}
}
