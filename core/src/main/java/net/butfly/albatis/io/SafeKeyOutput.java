package net.butfly.albatis.io;

import net.butfly.albacore.paral.Sdream;

public abstract class SafeKeyOutput<K, V> extends SafeOutputBase<V> implements KeyOutput<K, V> {
	protected SafeKeyOutput(String name) {
		super(name);
	}

	protected abstract void enqSafe(K key, Sdream<V> items);

	@Override
	protected final void enqSafe(Sdream<V> items) {
		items.partition((k, ss) -> enqueue(k, ss), v -> partition(v), 1000);
	}

	@Override
	public final void enqueue(K key, Sdream<V> v) {
		enqSafe(key, v);
	}

	@Override
	public final void enqueue(Sdream<V> s) {
		s.partition((k, ss) -> enqSafe(k, ss), v -> partition(v), 1000);
	}
}
