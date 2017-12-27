package net.butfly.albatis.io;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;

public abstract class SafeKeyOutput<K, V> extends SafeOutputBase<V> implements KeyOutput<K, V> {
	protected SafeKeyOutput(String name) {
		super(name);
	}

	protected abstract void enqSafe(K key, Sdream<V> items);

	@Override
	public final void enqueue(K key, Sdream<V> v) {
		opsPending.incrementAndGet();
		Exeter.of().execute(() -> enqSafe(key, v));
	}

	@Override
	public final void enqueue(Sdream<V> s) {
		s.partition((k, ss) -> enqueue(k, ss), v -> partition(v), 1000);
	}

	@Override
	protected void enqSafe(Sdream<V> s) {
		s.partition((k, ss) -> enqueue(k, ss), v -> partition(v), 1000);
	}
}
