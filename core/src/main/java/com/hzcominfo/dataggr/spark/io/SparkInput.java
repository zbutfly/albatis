package com.hzcominfo.dataggr.spark.io;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.hzcominfo.dataggr.spark.integrate.Client;
import com.hzcominfo.dataggr.spark.util.BytesUtils;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Message;

public class SparkInput extends Namedly implements Input<Message>, Serializable {
	private static final long serialVersionUID = 6966901980613011951L;
	private final Dataset<Row> dataset;
	private final Client client;
	private final LinkedBlockingDeque<Message> pool = new LinkedBlockingDeque<>();
	private StreamingQuery query;

	public SparkInput(String name, URISpec uriSpec) {
		super(name);
		this.client = new Client(name, uriSpec);

		// TODO read
		List<String> kfields = new ArrayList<>();
		kfields.add("CERTIFIED_ID");
		kfields.add("TRAIN_DAY");
		dataset = kafka(client.read(), kfields);
		// TODO rule config
		
		closing(this::close);
		open();
	}

	@Override
	public void open() {
		this.query = dataset.writeStream().foreach(new ForeachWriter<Row>() {
			private static final long serialVersionUID = 3602739322755312373L;

			@Override
			public void process(Row row) {
				StructType schema = row.schema();
				String[] fieldNames = schema.fieldNames();
				Map<String, Object> map = new HashMap<>();
				for (String fn : fieldNames) {
					map.put(fn, row.getAs(fn));
				}
				if (!map.isEmpty()) {
					pool.offer(new Message(map));					
					System.out.println(map);
				}
			}

			@Override
			public boolean open(long partitionId, long version) {
				return true;
			}

			@Override
			public void close(Throwable err) {
			}
		}).start();
		Input.super.open();
	}

	@Override
	public void close() {
		try {
			this.query.awaitTermination();
		} catch (StreamingQueryException e) {
			throw new RuntimeException("SparkInput close error: " + e);
		}
		client.close();
		Input.super.close();
	}

	@Override
	public void dequeue(Consumer<Sdream<Message>> using) {
		List<Message> m = new CopyOnWriteArrayList<>();
		pool.drainTo(m, 100);// TODO
		if (!m.isEmpty())
			using.accept(Sdream.of(m));
	}

	private static Dataset<Row> kafka(Dataset<Row> dataset, List<String> fields) {
		List<StructField> sfs = new ArrayList<>();
		fields.forEach(t -> sfs.add(DataTypes.createStructField(t, DataTypes.StringType, true)));
		StructType st = DataTypes.createStructType(sfs);

		return dataset.map(r -> {
			byte[] bytes = r.getAs("value");
			Map<String, Object> der = BytesUtils.der(bytes);
			@SuppressWarnings("unchecked")
			Map<String, Object> value = (Map<String, Object>) der.get("value");
			Object[] arr = new Object[st.fieldNames().length];
			for (int i = 0; i < fields.size(); i++)
				arr[i] = value.get(fields.get(i));
			return RowFactory.create(arr);
		}, RowEncoder.apply(st));
	}
}
