package net.butfly.albatis.io;

import static net.butfly.albacore.utils.collection.Streams.list;
import static net.butfly.albacore.utils.collection.Streams.map;
import static net.butfly.albacore.utils.collection.Streams.spatialMap;
import static net.butfly.albacore.utils.parallel.Parals.eachs;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import net.butfly.albacore.io.Dequeuer;
import net.butfly.albacore.io.IO;
import net.butfly.albacore.utils.collection.Its;
import net.butfly.albacore.utils.collection.Streams;
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
		return Wrapper.wrap(this, "Then", (using, batchSize) -> dequeue(s -> using.accept(s.filter(Streams.NOT_NULL).map(conv)),
				batchSize));
	}

	default <V1> Input<V1> thens(Function<Iterable<V>, Iterable<V1>> conv) {
		return thens(conv, 1);
	}

	default <V1> Input<V1> thens(Function<Iterable<V>, Iterable<V1>> conv, int parallelism) {
		return Wrapper.wrap(this, "Thens", (using, batchSize) -> dequeue(//
				s -> eachs(list(spatialMap(s, parallelism, t -> conv.apply(() -> Its.it(t)).spliterator())), s1 -> using.accept(s1)),
				batchSize));
	}

	// more extends
	default PrefetchInput<V> prefetch(Queue<V> pool, int batchSize) {
		return new PrefetchInput<V>(this, pool, batchSize);
	}

	// constructor
	public static <T> Input<T> of(Iterator<? extends T> it) {
		return (using, batchSize) -> {
			Stream.Builder<T> b = Stream.builder();
			long count = 0;
			T t = null;
			while (it.hasNext()) {
				if ((t = it.next()) != null) {
					b.add(t);
					if (++count > batchSize) break;
				}
			}
			using.accept(b.build());
		};
	}

	public static <T> Input<T> of(Iterable<? extends T> collection) {
		return of(collection.iterator());
	}

	public static <T> Input<T> of(Supplier<? extends T> next, Supplier<Boolean> ending) {
		return of(Its.it(next, ending));
	}

	@Override
	default V get() {
		AtomicReference<V> ref = new AtomicReference<>();
		dequeue(s -> ref.lazySet(s.findFirst().orElse(null)), 1);
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
