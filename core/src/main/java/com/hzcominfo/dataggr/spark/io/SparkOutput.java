package com.hzcominfo.dataggr.spark.io;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Output;

public abstract class SparkOutput extends SparkIO implements Output<Message>, Serializable {
	private static final long serialVersionUID = 7339834746933783020L;

	protected SparkOutput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	private Dataset<Row> conv(Sdream<Message> ms) {
		return null;// TODO: ms -> rows -> dataset
	}

	public abstract void enqueue(Dataset<Row> dataset);

	@Override
	public final void enqueue(Sdream<Message> items) {
		enqueue(conv(items));
	}
}
