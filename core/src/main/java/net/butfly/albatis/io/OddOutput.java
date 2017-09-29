package net.butfly.albatis.io;

import static net.butfly.albacore.paral.Task.waitWhen;

import net.butfly.albacore.paral.Sdream;

public interface OddOutput<V> extends Output<V> {
	boolean enqueue(V v);

	@Override
	public default void enqueue(Sdream<V> items) {
		if (!waitWhen(() -> full())) return;
		items.eachs(v -> {
			if (enqueue(v)) succeeded(1);
			else failed(v);
		});
	}

	@Override
	public final void enqueue(Stream<V> items) {
		if (!Concurrents.waitSleep(() -> full())) return;
		AtomicLong c = new AtomicLong(0);
		Streams.of(items).forEach(t -> {
			if (enqueue(t)) c.incrementAndGet();
		});
		succeeded(c.get());
	}

	default void failed(V v) {}
}
