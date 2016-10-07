package net.butfly.albacore.io;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.logger.Logger;

public final class OffHeapMapQueue extends MapQueueImpl<String, byte[], OffHeapQueue> {
	private static final long serialVersionUID = 1135380051081571780L;
	protected static final Logger logger = Logger.getLogger(OffHeapMapQueue.class);

	@SafeVarargs
	public OffHeapMapQueue(String name, String folder, Converter<byte[], String> keying, long capacity, String... keys) {
		super(name, keying, k -> new OffHeapQueue(k, folder, capacity), capacity, keys);
	}

	@Override
	public long statsSize(byte[] e) {
		return null == e ? Statistical.SIZE_NULL : e.length;
	}
}
