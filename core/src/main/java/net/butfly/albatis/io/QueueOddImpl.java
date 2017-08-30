package net.butfly.albatis.io;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.collection.Streams;
import net.butfly.albacore.utils.parallel.Concurrents;

public abstract class QueueOddImpl<V> extends Namedly implements Queue<V> {
	private final AtomicLong capacity;

	protected QueueOddImpl(long capacity) {
		super();
		this.capacity = new AtomicLong(capacity);
	}

	protected QueueOddImpl(String name, long capacity) {
		super(name);
		this.capacity = new AtomicLong(capacity);
	}

	protected abstract boolean enqueue(V item);

	@Override
	public long enqueue(Stream<V> items) {
		if (!Concurrents.waitSleep(() -> full())) return 0;
		AtomicLong c = new AtomicLong(0);
		Streams.of(items).forEach(t -> {
			if (enqueue(t)) c.incrementAndGet();
		});
		return c.get();
	}

	protected V dequeue() {
		AtomicReference<V> ref = new AtomicReference<>();
		return 0 == dequeue(s -> {
			ref.set(s.findFirst().orElse(null));
			return 1L;
		}, 1) ? null : ref.get();
	}

	@Override
	public long dequeue(Function<Stream<V>, Long> using, int batchSize) {
		return using.apply(Streams.of(() -> dequeue(), batchSize, () -> empty() && opened()));
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
