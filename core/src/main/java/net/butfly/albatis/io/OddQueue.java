package net.butfly.albatis.io;

import java.util.concurrent.atomic.AtomicLong;

public abstract class OddQueue<V> extends OddOutputBase<V> implements Queue<V>, OddInput<V> {
	private final AtomicLong capacity;

	protected OddQueue(String name, long capacity) {
		super(name);
		this.capacity = new AtomicLong(capacity);
	}

	@Override
	public final long capacity() {
		return capacity.get();
	}

	@Override
	public String toString() {
		return name() + "[" + size() + "]";
	}
}
