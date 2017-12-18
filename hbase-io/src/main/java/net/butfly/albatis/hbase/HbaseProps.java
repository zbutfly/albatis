package net.butfly.albatis.hbase;

public @interface HbaseProps {
	final String OUTPUT_CONCURRENT_OPS = "albatis.hbase.concurrent.ops.limit";
	final String OUTPUT_BATCH_SIZE = "albatis.hbase.concurrent.ops.limit";
	final String INPUT_BATCH_SIZE = "albatis.hbase.concurrent.ops.limit";
}
