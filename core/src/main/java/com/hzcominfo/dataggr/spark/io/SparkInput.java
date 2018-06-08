package com.hzcominfo.dataggr.spark.io;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import com.hzcominfo.dataggr.spark.util.RowConsumer;

import net.butfly.albacore.exception.NotImplementedException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.OddInput;

public abstract class SparkInput extends SparkIO implements OddInput<Row>, Serializable {
	private static final long serialVersionUID = 6966901980613011951L;
	Dataset<Row> dataset;

	public SparkInput() {
		super();
	}

	protected SparkInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	@Override
	public void open() {
		OddInput.super.open();
//		dataset = read();
	}

	public abstract StreamingQuery read(RowConsumer using);

	@Override
	public void close() {
		OddInput.super.close();
		spark.close();
	}

	@Override
	public Row dequeue() {
		throw new UnsupportedOperationException();
		// using.accept(conv(dataset));
	}

	// TODO
	public SparkInput join(String selfKeyFieldName, Map<String, SparkInput> sencendaryInput) {
		throw new NotImplementedException();
		// return new SparkJoinedInput(...);
	}

}
