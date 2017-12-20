package net.butfly.albatis.elastic;

public @interface ElasticProps {
	final String OUTPUT_CONCURRENT_OPS = "albatis.es.concurrent.ops.limit";
	final String OUTPUT_BATCH_SIZE = "albatis.es.concurrent.ops.limit";
	final String INPUT_BATCH_SIZE = "albatis.es.concurrent.ops.limit";
}
