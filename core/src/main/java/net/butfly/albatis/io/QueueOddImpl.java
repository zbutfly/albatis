package net.butfly.albatis.io;

import static net.butfly.albacore.paral.Task.waitSleep;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.steam.Steam;
import net.butfly.albacore.utils.collection.Streams;

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
		failed(Steam.of(t));
	}

	@Override
	public void enqueue(Steam<V> items) {
		if (!waitSleep(() -> full())) return;
		Streams.of(items).forEach(t -> {
			if (enqueue(t)) succeeded(1);
			else failed(t);
		});

	}

	protected abstract V dequeue();

	@Override
	public void dequeue(Consumer<Steam<V>> using, int batchSize) {
		using.accept(Steam.of(() -> dequeue(), batchSize, () -> empty() && opened()));
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
