package net.butfly.albatis.kudu;

public @interface KuduProps {
	final String TABLE_BUCKETS = "albatis.kudu.table.buckets";
	final String TABLE_REPLICAS = "albatis.kudu.table.replicas";
	final String TIMEOUT = "albatis.kudu.timeout";

	final String OUTPUT_CONCURRENT_OPS = "albatis.solr.concurrent.ops.limit";
	final String OUTPUT_BATCH_SIZE = "albatis.solr.concurrent.ops.limit";
	final String INPUT_BATCH_SIZE = "albatis.solr.concurrent.ops.limit";
}
