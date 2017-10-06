package net.butfly.albatis.io;

import static net.butfly.albacore.utils.collection.Streams.of;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;

public abstract class OddInput<V> extends Namedly implements Input<V>, Supplier<V>, Iterator<V> {
	protected OddInput() {
		super();
	}

	protected OddInput(String name) {
		super(name);
	}

	protected abstract V dequeue();

	@Override
	public final void dequeue(Consumer<Stream<V>> using, int batchSize) {
		using.accept(Streams.of(this::dequeue, batchSize, () -> empty() && opened()).filter(Streams.NOT_NULL));
	}

	@Override
	public V get() {
		return dequeue();
	}
}
