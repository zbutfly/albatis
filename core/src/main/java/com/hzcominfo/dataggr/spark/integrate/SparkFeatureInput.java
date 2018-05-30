package com.hzcominfo.dataggr.spark.integrate;

import java.util.List;
import java.util.function.Consumer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Message;

/**
 * test
 * 
 * @author chenw
 *
 */
public class SparkFeatureInput implements Input<Message> {
	private final Client client;
	private final Dataset<Row> dataset;
	private final Sdream<Message> sdream;

	public SparkFeatureInput(String name, String bootstrapUrl) {
		this(name, new URISpec(bootstrapUrl));
	}

	public SparkFeatureInput(String name, URISpec uriSpec) {
		client = new Client(name, uriSpec);
		dataset = client.read();
//		dataset.limit(10);
		sdream = shape();
		closing(this::close);
	}
	
	/*static Function<Row, Message> fun = row -> {
		StructType schema = row.schema();
		String[] fieldNames = schema.fieldNames();
		Map<String, Object> map = new HashMap<>();
		for (String fn : fieldNames) {
			map.put(fn, row.getAs(fn));
		}
		System.out.println(map); //
		return new Message(map);
	};*/
	
	private Sdream<Message> shape() {
		if (dataset == null)
			return null;
		MyForeachWriter writer = new MyForeachWriter();
		if (dataset.isStreaming()) {
			dataset.writeStream().foreach(writer).start();
			List<Message> ms = writer.getMs();
			return Sdream.of(ms);
		}
		return null;
	}
	
	@Override
	public void dequeue(Consumer<Sdream<Message>> using) {
		using.accept(sdream);
	}
 	
	@Override
	public void close() {
		client.close();
	}
}
