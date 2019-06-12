package net.butfly.albatis.kudu;

public @interface KuduProps {
	final String MODULE_NAME = "kudu";
	final String TABLE_BUCKETS = "albatis." + MODULE_NAME + ".table.buckets";
	final String TABLE_REPLICAS = "albatis." + MODULE_NAME + ".table.replicas";
	final String TIMEOUT = "albatis." + MODULE_NAME + ".timeout";

	final String OUTPUT_CONCURRENT_OPS = "albatis." + MODULE_NAME + ".concurrent.ops.limit";
	final String OUTPUT_BATCH_SIZE = "albatis." + MODULE_NAME + ".concurrent.ops.limit";
	final String INPUT_BATCH_SIZE = "albatis." + MODULE_NAME + ".concurrent.ops.limit";
	final String INPUT_STATS_STEP = "albatis." + MODULE_NAME + ".input.stats.step";
	final String OUTPUT_STATS_STEP = "albatis." + MODULE_NAME + ".output.stats.step";
	
	final String STUMBLEUPON_TIMEOUT = "albatis." + MODULE_NAME + ".output.stumbleupon.timeout";
}
