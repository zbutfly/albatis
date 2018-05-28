package com.hzcominfo.dataggr.spark.integrate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.types.StructType;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Message;

/**
 * test
 * @author chenw
 *
 */
public class SparkFeatureInput implements Input<Message> {
	private final Client client;
	private final Dataset<Row> dataset;
	public SparkFeatureInput(String name, String bootstrapUrl) {
		this(name, new URISpec(bootstrapUrl));
	}

	public SparkFeatureInput(String name, URISpec uriSpec) {
		client = new Client(name, uriSpec);
		dataset = client.read();
		closing(this::close);
	}
	
	@Override
	public void dequeue(Consumer<Sdream<Message>> using) {
		if (dataset == null) return;
		if (dataset.isStreaming()) {
			DataStreamWriter<Row> s = dataset.writeStream();
			s.foreach(new ForeachWriter<Row>() {
				private static final long serialVersionUID = 6359536691829860629L;

				@Override
				public void close(Throwable arg0) {
					// TODO Auto-generated method stub

				}

				@Override
				public boolean open(long arg0, long arg1) {
					// TODO Auto-generated method stub
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
					System.out.println(map); // 
					Message message = new Message(map);
					List<Message> ms = Colls.list();
					ms.add(message);
					using.accept(Sdream.of(ms));
				}
			});
		} else {
			dataset.foreach(row -> {
				StructType schema = row.schema();
				String[] fieldNames = schema.fieldNames();
				Map<String, Object> map = new HashMap<>();
				for (String fn : fieldNames) {
					map.put(fn, row.getAs(fn));
				}
				System.out.println(map); // 
				Message message = new Message(map);
				List<Message> ms = Colls.list();
				ms.add(message);
				using.accept(Sdream.of(ms));
			});
		}
		
	}

	@Override
	public void close() {
		client.close();
	}
}
