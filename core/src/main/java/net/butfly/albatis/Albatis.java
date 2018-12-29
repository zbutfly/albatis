package net.butfly.albatis;

public @interface Albatis {
	@Deprecated
	final String MAX_CONCURRENT_OP_FIELD_NAME = "MAX_CONCURRENT_OP_PROP_NAME";
	@Deprecated
	final String MAX_CONCURRENT_OP_FIELD_NAME_DEFAULT = "MAX_CONCURRENT_OP_DEFAULT";
	@Deprecated
	final String PROP_PUMP_BATCH_SIZE = "albatis.pump.batch.size";
	final String PROP_DEBUG_INPUT_LIMIT = "albatis.input.debug.limit";
}
