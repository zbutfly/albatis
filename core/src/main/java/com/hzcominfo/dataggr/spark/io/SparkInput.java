package com.hzcominfo.dataggr.spark.io;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.exception.NotImplementedException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Input;


public abstract class SparkInput extends SparkIO implements Input<Row>, Serializable {
	private static final long serialVersionUID = 6966901980613011951L;
	Dataset<Row> dataset;

	public SparkInput() {
		super();
	}

	protected SparkInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
		dataset = read();
	}

	public abstract Dataset<Row> read();

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
