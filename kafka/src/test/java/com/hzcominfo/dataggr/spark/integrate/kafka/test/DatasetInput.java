package com.hzcominfo.dataggr.spark.integrate.kafka.test;

import java.util.function.Consumer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.streaming.DataStreamWriter;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Input;

public class DatasetInput<R> implements Input<R> {
	Dataset<R> dataset;

	@Override
	public void dequeue(Consumer<Sdream<R>> using) {
		if (dataset == null) return;
		DataStreamWriter<R> s = dataset.writeStream();
		s.foreach(new ForeachWriter<R>() {
			private static final long serialVersionUID = 6359536691829860629L;

			@Override
			public void close(Throwable arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public boolean open(long arg0, long arg1) {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public void process(R r) {
				using.accept(Sdream.of(r));
			}
		});
	}
}
