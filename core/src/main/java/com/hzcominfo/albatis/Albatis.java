package com.hzcominfo.albatis;

public @interface Albatis {
	@interface Props {
		final String PROP_PUMP_BATCH_SIZE = "albatis.pump.batch.size";
		final String PROP_KUDU_TABLE_BUCKETS = "albatis.kudu.table.buckets";
		final String PROP_KUDU_TABLE_REPLICAS = "albatis.kudu.table.replicas";
		final String PROP_KUDU_TIMEOUT = "albatis.kudu.timeout";
		
		final String PROP_SOLR_CONCURRENT_OPS_LIMIT = "albatis.solr.concurrent.ops.limit";
	}

	final String MAX_CONCURRENT_OP_FIELD_NAME = "MAX_CONCURRENT_OP_PROP_NAME";
	final String MAX_CONCURRENT_OP_FIELD_NAME_DEFAULT = "MAX_CONCURRENT_OP_DEFAULT";
}
