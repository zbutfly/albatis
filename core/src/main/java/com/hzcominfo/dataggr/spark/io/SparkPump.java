package com.hzcominfo.dataggr.spark.io;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.pump.Pump;

public class SparkPump extends Namedly implements Pump<Map<String, Object>>, Serializable {
	private static final long serialVersionUID = -6842560101323305087L;
	private static final Logger logger = Logger.getLogger(SparkPump.class);

	private final SparkInput input;
	private final SparkOutput output;

	private final DataStreamWriter<Row> writing;

	public SparkPump(SparkInput input, SparkOutput output) {
		super(input.name() + ">" + output.name());
		this.input = input;
		this.output = output;
		Reflections.noneNull("Pump source/destination should not be null", input, output);
		writing = input.dataset.isStreaming() ? input.dataset.writeStream().foreach(output.writer) : null;
	}

	@Override
	public void open() {
		output.open();
		Pump.super.open();
		input.open();
		if (null != writing) {
			StreamingQuery q = writing.start();
			logger.info("Spark streaming pumping started.");
			input.closing(() -> {
				try {
					logger.info("Spark streaming pumping terminating...");
					q.awaitTermination();
				} catch (StreamingQueryException e) {}
			});
		} else input.dataset.foreach(output::write);
	}

	@Override
	public void close() {
		input.close();
		Pump.super.close();
		output.close();
	}
}
