package com.hzcominfo.dataggr.spark.integrate.kafka.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import com.hzcominfo.dataggr.spark.integrate.Client;
import com.hzcominfo.dataggr.spark.util.BytesUtils;

import net.butfly.albacore.io.URISpec;

public class KafkaWriteTest {

	public static void main(String[] args) {
		//ZHK_QBZX_LGZS_NEW 无数据
		//HZGA_WA_SOURCE_FJ_1001 5/30有数据
		URISpec uri = new URISpec("bootstrap://data01:9092,data02:9092,data03:9092/ZHW_TLGA_GNSJ_NEW");
		Client client = new Client("kafka-apptest", uri);
		Dataset<Row> dataset = client.read();

		dataset.writeStream().foreach(new ForeachWriter<Row>() {
			private static final long serialVersionUID = -3227914841696508223L;

			@Override
			public void close(Throwable arg0) {
			}

			@Override
			public boolean open(long arg0, long arg1) {
				return true;
			}

			@Override
			public void process(Row row) {
				StructType schema = row.schema();
				String[] fieldNames = schema.fieldNames();
				Map<String, Object> map = new HashMap<>();
				for (String fn : fieldNames) {
					if ("value".equals(fn)) {
						byte[] bytes = row.getAs(fn);
						Map<String, Object> der = BytesUtils.der(bytes);
						System.out.println(der.get("value"));
						map.putAll(der);
//						System.out.println(der);
						/*Object object;
						try {
							object = BytesUtils.byteToObject(bytes);
						} catch (ClassNotFoundException e) {
							throw new RuntimeException("classnotfound: " + e);
						} catch (IOException e) {
							throw new RuntimeException("io: " + e);
						}
						System.out.println(object);*/
//						map.putAll(value);
					} else map.put(fn, row.getAs(fn));
				}
//				System.out.println(map);
			}
		}).start();
		
		//debug模式调试，改行代码打下断点
		client.close();
	}
}
