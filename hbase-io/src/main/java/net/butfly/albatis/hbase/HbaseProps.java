package net.butfly.albatis.hbase;

public @interface HbaseProps {
	final String MODULE_NAME = "hbase";
	final String OUTPUT_CONCURRENT_OPS = "albatis." + MODULE_NAME + ".concurrent.ops.limit";
	final String OUTPUT_BATCH_SIZE = "albatis." + MODULE_NAME + ".concurrent.ops.output.batch";
	final String INPUT_BATCH_SIZE = "albatis." + MODULE_NAME + ".concurrent.ops.input.batch";
	final String INPUT_STATS_STEP = "albatis." + MODULE_NAME + ".input.stats.step";
	final String OUTPUT_STATS_STEP = "albatis." + MODULE_NAME + ".output.stats.step";
}
