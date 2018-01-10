package net.butfly.albatis.io;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import net.butfly.albacore.io.Dequeuer;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.ext.PrefetchInput;

public interface Input<V> extends IO, Dequeuer<V> {
	static Input<?> NULL = using -> {};

	@Override
	default long size() {
		return Long.MAX_VALUE;
	}

	@Override
	default long capacity() {
		return 0;
	}

	default <V1> Input<V1> then(Function<V, V1> conv) {
		return Wrapper.wrap(this, "Then", using -> dequeue(s -> using.accept(s.map(conv))));
	}

	default <V1> Input<V1> thens(Function<Sdream<V>, Sdream<V1>> conv) {
		return Wrapper.wrap(this, "Thens", using -> dequeue(s -> using.accept(conv.apply(s))));
	}

	@Deprecated
	default <V1> Input<V1> thens(Function<Sdream<V>, Sdream<V1>> conv, int parallelism) {
		return Wrapper.wrap(this, "Thens", using -> //
		dequeue(s -> s.partition(v -> using.accept(conv.apply(v)), parallelism)));
	}

	default <V1> Input<V1> thenFlat(Function<V, Sdream<V1>> conv) {
		return Wrapper.wrap(this, "ThenFlat", (Dequeuer<V1>) using -> //
		dequeue(s -> using.accept(s.mapFlat(conv))));
	}

	// more extends
	default PrefetchInput<V> prefetch(Queue<V> pool) {
		return new PrefetchInput<V>(this, pool);
	}

	// constructor
	public static <T> Input<T> of(Collection<? extends T> collection, int batchSize) {
		return new Input<T>() {
			private final BlockingQueue<T> undly = new LinkedBlockingQueue<>(collection);

			@Override
			public void dequeue(Consumer<Sdream<T>> using) {
				final List<T> l = Colls.list();
				undly.drainTo(l, batchSize);
				if (!l.isEmpty()) using.accept(Sdream.of(l));
			}
		};
	}

	public static <T> Input<T> of(Supplier<? extends T> next, Supplier<Boolean> ending, int batchSize) {
		return using -> {
			final List<T> l = Colls.list();
			T t;
			while (!ending.get() && null != (t = next.get()) && l.size() < batchSize)
				l.add(t);
			using.accept(Sdream.of(l));
		};
	}
}
