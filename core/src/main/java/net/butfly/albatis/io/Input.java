package net.butfly.albatis.io;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import net.butfly.albacore.io.Dequeuer;
import net.butfly.albacore.io.IO;
import net.butfly.albacore.paral.Parals;
import net.butfly.albacore.paral.steam.Sdream;
import net.butfly.albatis.io.ext.PrefetchInput;

public interface Input<V> extends IO, Dequeuer<V> {
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

	default <V1> Input<V1> thens(Function<Sdream<V>, Sdream<V1>> conv) {
		return Wrapper.wrap(this, "Thens", (using, batchSize) -> dequeue(s -> using.accept(conv.apply(s)), batchSize));
	}

	default <V1> Input<V1> thens(Function<Sdream<V>, Sdream<V1>> conv, int parallelism) {
		return Wrapper.wrap(this, "Thens", (using, batchSize) -> //
		dequeue(s -> s.partition(v -> using.accept(conv.apply(v)), parallelism), batchSize));
	}

	default <V1> Input<V1> then(Function<V, V1> conv) {
		return Wrapper.wrap(this, "Then", using -> dequeue(s -> using.accept(s.map(conv))));
	}

	// constructor
	public static <T> Input<T> of(Collection<? extends T> collection) {
		return new Input<T>() {
			private final BlockingQueue<T> undly = new LinkedBlockingQueue<>(collection);

			@Override
			public void dequeue(final Consumer<Sdream<T>> using, final int batchSize) {
				final List<T> l = Parals.list();
				undly.drainTo(l, batchSize);
				if (!l.isEmpty()) using.accept(Sdream.of(l));
			}
		};
	}

	public static <T> Input<T> of(Supplier<? extends T> next, Supplier<Boolean> ending) {
		return (using, batchSize) -> {
			final List<T> l = Parals.list();
			T t;
			while (!ending.get() && null != (t = next.get()) && l.size() < batchSize)
				l.add(t);
			using.accept(Sdream.of(l));
		};
	}
}
