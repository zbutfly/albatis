package com.hzcominfo.albatis;

public interface Albatis {
	interface Props {
		final String PROP_PUMP_BATCH_SIZE = "albatis.pump.batch.size";
		final String PROP_KUDU_TABLE_BUCKETS = "albatis.kudu.table.buckets";
		final String PROP_KUDU_TIMEOUT = "albatis.kudu.timeout";
	}
}
