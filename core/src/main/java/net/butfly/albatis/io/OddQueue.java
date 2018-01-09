package net.butfly.albatis.io;

import java.util.concurrent.atomic.AtomicLong;

import net.butfly.albacore.base.Namedly;

public abstract class OddQueue<V> extends Namedly implements Queue<V>, OddInput<V>, OddOutput<V> {
	private final AtomicLong capacity;

	@Deprecated
	protected OddQueue(long capacity) {
		super();
		this.capacity = new AtomicLong(capacity);
	}

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
