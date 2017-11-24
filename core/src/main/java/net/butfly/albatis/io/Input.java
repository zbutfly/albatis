package net.butfly.albatis.io;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import net.butfly.albacore.io.Dequeuer;
import net.butfly.albacore.io.IO;
import net.butfly.albacore.paral.split.SplitEx;
import net.butfly.albacore.paral.steam.Steam;
import net.butfly.albatis.io.ext.PrefetchInput;

public interface Input<V> extends IO, Dequeuer<V>, Supplier<V>, Iterator<V> {
	static Input<?> NULL = (using, batchSize) -> {};

	@Override
	default long size() {
		return Long.MAX_VALUE;
	}

	@Override
	default long capacity() {
		return 0;
	}

	default <V1> Input<V1> then(Function<V, V1> conv) {
		return Wrapper.wrap(this, "Then", (using, batchSize) -> dequeue(s -> using.accept(s.map(conv)), batchSize));
	}

	default <V1> Input<V1> thens(Function<Steam<V>, Steam<V1>> conv) {
		return Wrapper.wrap(this, "Thens", (using, batchSize) -> dequeue(s -> using.accept(conv.apply(s)), batchSize));
	}

	default <V1> Input<V1> thens(Function<Steam<V>, Steam<V1>> conv, int parallelism) {
		return Wrapper.wrap(this, "Thens", (using, batchSize) -> //
		dequeue(s -> s.partition(v -> using.accept(conv.apply(v)), parallelism), batchSize));
	}

	// more extends
	default PrefetchInput<V> prefetch(Queue<V> pool, int batchSize) {
		return new PrefetchInput<V>(this, pool, batchSize);
	}

	// constructor
	public static <T> Input<T> of(Collection<? extends T> collection) {
		return new Input<T>() {
			private final BlockingQueue<T> undly = new LinkedBlockingQueue<>(collection);

			@Override
			public void dequeue(final Consumer<Steam<T>> using, final int batchSize) {
				final List<T> l = SplitEx.list();
				undly.drainTo(l, batchSize);
				if (!l.isEmpty()) using.accept(Steam.of(l));
			}
		};
	}

	public static <T> Input<T> of(Supplier<? extends T> next, Supplier<Boolean> ending) {
		return (using, batchSize) -> {
			final List<T> l = SplitEx.list();
			T t;
			while (!ending.get() && null != (t = next.get()) && l.size() < batchSize)
				l.add(t);
			using.accept(Steam.of(l));
		};
	}

	@Override
	default V get() {
		AtomicReference<V> ref = new AtomicReference<>();
		dequeue(s -> s.next(v -> ref.set(v)), 1);
		return ref.get();
	}

	@Override
	default boolean hasNext() {
		return !empty();
	}

	@Override
	default V next() {
		return get();
	}
}
