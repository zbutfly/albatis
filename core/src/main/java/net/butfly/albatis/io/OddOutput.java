package net.butfly.albatis.io;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.collection.Streams;
import net.butfly.albacore.utils.parallel.Concurrents;

public abstract class OddOutput<V> extends Namedly implements Output<V> {
	protected OddOutput() {
		super();
	}

	protected OddOutput(String name) {
		super(name);
	}

	protected abstract boolean enqueue(V item);

	@Override
	public final void enqueue(Stream<V> items) {
		if (!Concurrents.waitSleep(() -> full())) return;
		AtomicLong c = new AtomicLong(0);
		Streams.of(items).forEach(t -> {
			if (enqueue(t)) c.incrementAndGet();
		});
		succeeded(c.get());
	}
}
