package net.butfly.albacore.io;

import net.butfly.albacore.lambda.Converter;

public class OffHeapMapQueue<K> extends MapQueueImpl<K, byte[], OffHeapQueue> {
	private static final long serialVersionUID = 1135380051081571780L;

	protected OffHeapMapQueue(String name, Converter<byte[], K> keying, Converter<K, OffHeapQueue> constructor, long capacity) {
		super(name, keying, constructor, capacity);
	}
}
