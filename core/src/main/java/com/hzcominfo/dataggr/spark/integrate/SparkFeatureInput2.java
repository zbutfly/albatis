package com.hzcominfo.dataggr.spark.integrate;

import java.io.Serializable;
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
 * 
 * @author chenw
 *
 */
public class SparkFeatureInput2 implements Input<Message>, Serializable {
	private static final long serialVersionUID = -4742008582795468309L;
	private final Client client;
	private final Dataset<Row> dataset;

	public SparkFeatureInput2(String name, String bootstrapUrl) {
		this(name, new URISpec(bootstrapUrl));
	}

	public SparkFeatureInput2(String name, URISpec uriSpec) {
		client = new Client(name, uriSpec);
		dataset = client.read();
		closing(this::close);
	}

	private class ForeachWriter$anonfun$ extends ForeachWriter<Row> implements Serializable {
		private static final long serialVersionUID = -6782476040095757847L;

		public ForeachWriter$anonfun$(Consumer<Sdream<Message>> using) {
			super();
			this.using = using;
		}

		private Consumer<Sdream<Message>> using;

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
			Message message = new Message(map);
			List<Message> ms = Colls.list();
			ms.add(message);
			using.accept(Sdream.of(ms));
		}
	}

	private class Using implements Consumer<Sdream<Message>>, Serializable {
		private static final long serialVersionUID = -4119278416216117061L;
		private final Consumer<Sdream<Message>> base;

		public Using(Consumer<Sdream<Message>> base) {
			super();
			this.base = base;
		}

		@Override
		public void accept(Sdream<Message> t) {
			base.accept(t);
		}

	}

	@Override
	public void dequeue(Consumer<Sdream<Message>> using) {
		if (dataset == null)
			return;

		if (dataset.isStreaming()) {
			DataStreamWriter<Row> s = dataset.writeStream();
			ForeachWriter$anonfun$ fw = new ForeachWriter$anonfun$(new Using(using));
			s.foreach(fw);
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
