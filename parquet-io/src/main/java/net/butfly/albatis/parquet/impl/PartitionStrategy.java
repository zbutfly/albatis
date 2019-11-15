package net.butfly.albatis.parquet.impl;

import java.util.Map;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.ExtraTableDesc;

public abstract class PartitionStrategy implements ExtraTableDesc {
	protected int rollingRecord;
	protected long rollingByte;
	protected long rollingMS;
	protected long refreshMS;
	public String hdfsUri;
	public String jdbcUri;

	public PartitionStrategy() {
		super();
	}

	public abstract String partition(Map<String, Object> rec);

	@Override
	public Map<String, Object> extra() {
		return Maps.of(HiveParquetWriter.PARTITION_STRATEGY_IMPL_PARAM, this);
	}

	public int rollingRecord() {
		return rollingRecord;
	}

	public long rollingByte() {
		return rollingByte;
	}

	public long rollingMS() {
		return rollingMS;
	}

	public long refreshMS() {
		return refreshMS;
	}
}
