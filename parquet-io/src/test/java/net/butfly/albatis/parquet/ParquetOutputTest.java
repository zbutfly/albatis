package net.butfly.albatis.parquet;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.json.Jsonx;
import net.butfly.albatis.Connection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.ddl.vals.ValType;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.parquet.impl.HiveParquetWriter;
import net.butfly.albatis.parquet.impl.PartitionStrategy;
import net.butfly.albatis.parquet.impl.PartitionStrategyMultipleField;

public class ParquetOutputTest {
	private static final Random rd = new Random(); // creating Random object

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException {
		// java.nio.file.Path jbase = java.nio.file.Path.of("C:\\Temp\\parquets");
		// org.apache.hadoop.fs.Path hbase = new org.apache.hadoop.fs.Path(jbase.toUri());

		TableDesc td = TableDesc.dummy("test_table")//
		// .attw(HiveParquetWriter.ROLLING_RECORD_COUNT_PARAM, 1000)//
		// .attw(HiveParquetWriter.PARTITION_STRATEGY_DESC_PARAM, "DATE:yyyy/MM/dd/hh")//
		// .attw(HiveParquetWriter.PARTITION_FIELD_NAME_PARAM, "BIRTHDAY")//
		// TODO .attw(HiveParquetWriter.PARTITION_STRATEGY_IMPL_PARAM, new PartitionStrategyMultipleField("BIRTHDAY", "yyyy/MM/dd/hh", null))
		;
		PartitionStrategy s = new PartitionStrategyMultipleField((Map<String, Object>) Jsonx.map(STRATEGY_JSON));
		td.attw(HiveParquetWriter.PARTITION_STRATEGY_IMPL_PARAM, s);

		new FieldDesc(td, "NAME", ValType.STR);
		new FieldDesc(td, "AGE", ValType.INT);
		new FieldDesc(td, "BIRTHDAY", ValType.INT);
		List<Rmap> l = Colls.list();

		long now = new Date().getTime();
		try (Connection c = Connection.connect(new URISpec("hive:parquet:hdfs://127.0.0.1:11000/")); Output<Rmap> o = c.output(td);) {
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

	static private final String STRATEGY_JSON = "{\"errcode\":\"000200\",\"errmsg\":\"success\",\"responseObject\":{\"partitionConfig\":{\"maxRecordSize\":1000,\"maxFileSize\":128,\"maxTimeHour\":1.0},\"partitionColumnList\":[{\"partitionFieldType\":\"string\",\"srcFieldName\":\"ETL_TIMESTAMP_dt\",\"level\":1,\"srcFieldType\":\"date\",\"partitionFormat\":\"yyyy\",\"partitionFieldName\":\"year\"},{\"partitionFieldType\":\"string\",\"srcFieldName\":\"ETL_TIMESTAMP_dt\",\"level\":2,\"srcFieldType\":\"date\",\"partitionFormat\":\"MM\",\"partitionFieldName\":\"month\"},{\"partitionFieldType\":\"string\",\"srcFieldName\":\"ETL_TIMESTAMP_dt\",\"level\":3,\"srcFieldType\":\"date\",\"partitionFormat\":\"dd\",\"partitionFieldName\":\"day\"}]}}";
}
