package net.butfly.albatis.io;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
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

	protected void failed(V t) {
		failed(Streams.of(t));
	}

	@Override
	public void enqueue(Stream<V> items) {
		if (!Concurrents.waitSleep(() -> full())) return;
		Streams.of(items).forEach(t -> {
			if (enqueue(t)) succeeded(1);
			else failed(t);
		});

	}

	protected abstract V dequeue();

	@Override
	public void dequeue(Consumer<Stream<V>> using, int batchSize) {
		using.accept(Streams.of(() -> dequeue(), batchSize, () -> empty() && opened()));
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
