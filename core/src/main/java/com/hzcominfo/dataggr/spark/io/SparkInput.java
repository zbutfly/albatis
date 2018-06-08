package com.hzcominfo.dataggr.spark.io;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import net.butfly.albacore.exception.NotImplementedException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Input;

public abstract class SparkInput extends SparkIO implements Input<Row>, Serializable {
	private static final long serialVersionUID = 6966901980613011951L;
	private Dataset<Row> dataset;

	public SparkInput() {
		super();
	}

	protected SparkInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
		dataset = load();
	}

	public abstract Dataset<Row> load();

	public StreamingQuery start(ForeachWriter<Row> writer) {
		return dataset.writeStream().foreach(writer).start();
	}

	@Override
	public void close() {
		Input.super.close();
		spark.close();
	}

	@Override
	public final void dequeue(Consumer<Sdream<Row>> using) {
		throw new UnsupportedOperationException();
		// using.accept(conv(dataset));
	}

	// TODO
	public SparkInput join(String selfKeyFieldName, Map<String, SparkInput> sencendaryInput) {
		throw new NotImplementedException();
		// return new SparkJoinedInput(...);
	}
}
