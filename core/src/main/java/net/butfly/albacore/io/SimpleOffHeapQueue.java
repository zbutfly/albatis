package net.butfly.albacore.io;

import net.butfly.albacore.lambda.Converter;

public final class SimpleOffHeapQueue<V> extends OffHeapQueue<V, V> {
	public SimpleOffHeapQueue(String name, String dataFolder, long capacity, Converter<V, byte[]> conv, Converter<byte[], V> unconv) {
		super("off-heap-queue-" + name, dataFolder, capacity, conv, unconv);
	}
}
