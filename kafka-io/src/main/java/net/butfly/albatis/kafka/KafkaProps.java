package net.butfly.albatis.kafka;

public @interface KafkaProps {
	final String MODULE_NAME = "kafka";

	final String INPUT_BATCH_SIZE = "albatis." + MODULE_NAME + ".input.batch.size";
	final String INPUT_STATS_STEP = "albatis." + MODULE_NAME + ".input.stats.step";
	final String INPUT_CONCURRENT_OPS = "albatis." + MODULE_NAME + ".input.paral.limit";
	final String OUTPUT_BATCH_SIZE = "albatis." + MODULE_NAME + ".output.batch.size";
	final String OUTPUT_STATS_STEP = "albatis." + MODULE_NAME + ".output.stats.step";
	final String OUTPUT_CONCURRENT_OPS = "albatis." + MODULE_NAME + ".output.paral.limit";
}
