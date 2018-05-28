package com.hzcominfo.dataggr.spark.integrate.kafka.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import com.hzcominfo.dataggr.spark.integrate.Client;
import com.hzcominfo.dataggr.spark.integrate.ReadConfig;

import net.butfly.albacore.io.URISpec;

public class AppTest {

	public static void main(String[] args) {
		URISpec uri = new URISpec("bootstrap://data01:9092,data02:9092,data03:9092/ZHK_QBZX_LGZS_NEW");
		spark(uri); // new Class1(URISpec uri)
	}
	
	static void spark(URISpec uri) {
		Client client = new Client(null ,uri);
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
					map.put(fn, row.getAs(fn));
				}
				System.out.println(map);
//				System.out.println("Processing ${row}" + row);
			}
		}).start();
//		dataset.writeStream().format("console").start();
//		dataset.printSchema();
//		List<Iterator<Row>> list = new ArrayList<>();
//		dataset.toJavaRDD().foreachPartition(t -> list.add(t));
//		System.out.println(list.get(0).next());
		/*Iterator<Row> localIterator = dataset.javaRDD().toLocalIterator();
		while(localIterator.hasNext()) {
			Row row = localIterator.next();
			System.err.println(row.get(0));
		}*/
		client.close();
		
	}
	
	static ReadConfig getConfig(URISpec uri) {
		switch(uri.getScheme()) {
		case "bootstrap":
			Map<String, String> options = new HashMap<>();
			options.put("kafka.bootstrap.servers", uri.getHost());
			options.put("subscribe", uri.getFile());
			options.put("startingOffsets", "earliest");
			return new ReadConfig("kafka", options, true);
		default:
			break;
		}
		return null;
	}
}
