package net.butfly.albatis.parquet;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.Connection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.ddl.vals.ValType;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.parquet.impl.HiveParquetWriter;

public class ParquetOutputTest {
	private static final Random rd = new Random(); // creating Random object

	public static void main(String[] args) throws IOException {
		// java.nio.file.Path jbase = java.nio.file.Path.of("C:\\Temp\\parquets");
		// org.apache.hadoop.fs.Path hbase = new org.apache.hadoop.fs.Path(jbase.toUri());

		TableDesc td = TableDesc.dummy("test_table")//
				.attw(HiveParquetWriter.ROLLING_RECORD_COUNT_PARAM, 1000)//
				.attw(HiveParquetWriter.HASHING_FIELD_NAME_PARAM, "BIRTHDAY")//
				.attw(HiveParquetWriter.HASHING_STRATEGY_DESC_PARAM, "DATE:yyyy/MM/dd/hh");
		new FieldDesc(td, "NAME", ValType.STR);
		new FieldDesc(td, "AGE", ValType.INT);
		new FieldDesc(td, "BIRTHDAY", ValType.INT);
		List<Rmap> l = Colls.list();

		long now = new Date().getTime();
		try (Connection c = Connection.connect(new URISpec("hive:parquet:file:///C:/Temp/parquets")); Output<Rmap> o = c.output(td);) {
			for (int i = 0; i < 99; i++) {
				l.clear();
				for (int j = 0; j < 120; j++) {
					Rmap r = new Rmap(td.qualifier);
					r.put("NAME", "zx-" + i + "-" + j + "-@" + rd.nextInt(100) + 20);
					r.put("AGE", rd.nextInt(100) + 20);
					r.put("BIRTHDAY", new Date(now - Math.abs(rd.nextLong()) % 20 * 1000 * 3600 * 24));
					l.add(r);
				}
				o.enqueue(Sdream.of(l));
			}
		}
	}

}
