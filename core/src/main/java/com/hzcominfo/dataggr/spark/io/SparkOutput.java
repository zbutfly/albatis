package com.hzcominfo.dataggr.spark.io;

import java.io.Serializable;
import java.util.function.Consumer;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Output;

public abstract class SparkOutput extends SparkIO implements Output<Row>, Serializable {
	private static final long serialVersionUID = 7339834746933783020L;
	protected ForeachWriter<Row> writer;

	public SparkOutput() {}

	protected SparkOutput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
		writer = writerFor(this::write);
	}

	public abstract void write(Row row);

	@Override
	public final void enqueue(Sdream<Row> items) {
		items.eachs(this::write);
	}

	public static <T> ForeachWriter<T> writerFor(Consumer<T> using) {
		return new ForeachWriter<T>() {
			private static final long serialVersionUID = 3602739322755312373L;

			@Override
			public void process(T m) {
				using.accept(m);
			}

			@Override
			public boolean open(long partitionId, long version) {
				return true;
			}

			@Override
			public void close(Throwable err) {}
		};
	}
}
