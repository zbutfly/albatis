package net.butfly.albatis.io;

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.collection.Streams;

public abstract class InputOddImpl<V> extends Namedly implements Input<V>, Supplier<V>, Iterator<V> {
	protected InputOddImpl() {
		super();
	}

	protected InputOddImpl(String name) {
		super(name);
	}

	protected abstract V dequeue();

	@Override
	public final long dequeue(Function<Stream<V>, Long> using, int batchSize) {
		return using.apply(Streams.of(() -> dequeue(), batchSize, () -> empty() && opened()));
	}

	@Override
	public V get() {
		return dequeue();
	}
}
