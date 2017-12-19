package net.butfly.albatis.io;

import java.util.concurrent.atomic.AtomicInteger;

import net.butfly.albacore.paral.Sdream;

public abstract class SafeKeyOutput<K, V> extends SafeOutputBase<V> implements KeyOutput<K, V> {
	protected SafeKeyOutput(String name) {
		super(name);
	}

	protected abstract void enqueue(K key, Sdream<V> items, AtomicInteger ops);

	@Override
	protected final void enqueue(Sdream<V> items, AtomicInteger ops) {
		items.partition((k, ss) -> enqueue(k, ss, ops), v -> partition(v), 1000);
	}

	@Override
	public final void enqueue(K key, Sdream<V> v) {
		enqueue(key, v, currOps);
	}

	@Override
	public final void enqueue(Sdream<V> s) {
		s.partition((k, ss) -> enqueue(k, ss, currOps), v -> partition(v), 1000);
	}
}
