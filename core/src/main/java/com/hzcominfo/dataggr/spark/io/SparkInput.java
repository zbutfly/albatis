package com.hzcominfo.dataggr.spark.io;

import java.io.Serializable;
import java.util.function.Consumer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Message;

public abstract class SparkInput extends SparkIO implements Input<Message>, Serializable {
	private static final long serialVersionUID = 6966901980613011951L;
	private final Dataset<Row> dataset;

	protected SparkInput(SparkSession spark, URISpec targetUri) {
		this(spark, targetUri, new String[] {});
	}

	protected SparkInput(SparkSession spark, URISpec targetUri, String... fields) {
		super(spark, targetUri, fields);
		dataset = dequeue();
	}

	@Override
	public void close() {
		Input.super.close();
		spark.close();
	}

	protected abstract Dataset<Row> dequeue();
	
	public Dataset<Row> load() {
		return dataset;
	};

	@Override
	public final void dequeue(Consumer<Sdream<Message>> using) {
		using.accept(conv(dequeue()));
	}

	private Sdream<Message> conv(Dataset<Row> dequeue) {
		// TODO Auto-generated method stub
		return null;
	}

	public StreamingQuery start(ForeachWriter<Row> writer) {
		return dataset.writeStream().foreach(writer).start();
	}

	protected abstract Row selectItems(Row r);

	protected Row defaultSelectItems(Row r) {
		Object[] arr = new Object[fields.length];
		for (int i = 0; i < fields.length; i++)
			arr[i] = r.getAs(fields[i]);
		return RowFactory.create(arr);
	}

	protected Dataset<Row> map(Dataset<Row> dataset) {
		return dataset.map(this::selectItems, RowEncoder.apply(super.getType()));
	}
}
