package com.hzcominfo.dataggr.spark.io;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.pump.Pump;

public class SparkPump extends Namedly implements Pump<Message>, Serializable {
	private static final long serialVersionUID = -6842560101323305087L;

	private final SparkInput input;
	private final SparkOutput output;
	private final ForeachWriter<Row> writer;
	private final StreamingQuery queue;

	public SparkPump(SparkInput input, SparkOutput output) {
		super(input.name() + ">" + output.name());
		this.input = input;
		this.output = output;
		Reflections.noneNull("Pump source/destination should not be null", input, output);
		writer = new ForeachWriter<Row>() {
			private static final long serialVersionUID = 3602739322755312373L;

			@Override
			public void process(Row row) {
				StructType schema = row.schema();
				String[] fieldNames = schema.fieldNames();
				Map<String, Object> map = new HashMap<>();
				for (String fn : fieldNames)
					map.put(fn, row.getAs(fn));
				if (map.isEmpty()) return;
				output.enqueue(Sdream.of(new Message[] { new Message(map) }));
			}

			@Override
			public boolean open(long partitionId, long version) {
				return true;
			}

			@Override
			public void close(Throwable err) {}
		};
		queue = input.start(writer);
		input.closing(() -> {
			try {
				queue.awaitTermination();
			} catch (StreamingQueryException e) {}
		});
	}

	@Override
	public void open() {
		output.open();
		Pump.super.open();
		input.open();
	}

	@Override
	public void close() {
		input.close();
		Pump.super.close();
		output.close();
	}
}
