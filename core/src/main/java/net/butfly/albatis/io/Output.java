package net.butfly.albatis.io;

import static net.butfly.albacore.utils.collection.Streams.batching;
import static net.butfly.albacore.utils.collection.Streams.map;
import static net.butfly.albacore.utils.collection.Streams.spatial;
import static net.butfly.albacore.utils.parallel.Parals.eachs;

import java.util.Collection;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import net.butfly.albacore.io.Enqueuer;
import net.butfly.albacore.io.IO;
import net.butfly.albacore.utils.collection.Its;
import net.butfly.albacore.utils.collection.Streams;
import net.butfly.albatis.io.ext.FailoverOutput;

public interface Output<V> extends IO, Consumer<Stream<V>>, Enqueuer<V> {
	static Output<?> NULL = items -> {};

	@Override
	default long size() {
		return 0;
	}

	@Override
	default void accept(Stream<V> items) {
		enqueue(items);
	}

	default <V0> Output<V0> prior(Function<V0, V> conv) {
		return Wrapper.wrap(this, "Prior", s -> enqueue(Streams.of(s.filter(Streams.NOT_NULL).map(conv))));
	}

	default <V0> Output<V0> priors(Function<Iterable<V0>, Iterable<V>> conv) {
		return priors(conv, 1);
	}

	default <V0> Output<V0> priors(Function<Iterable<V0>, Iterable<V>> conv, int parallelism) {
		return Wrapper.wrap(this, "Priors", s -> Parals.eachs(Streams.spatial(s, parallelism).values(), s0 -> enqueue(Streams.of(conv.apply(
				(Iterable<V0>) () -> Its.it(s0)))), Streams.LONG_SUM));
	}

	// more extends

	default FailoverOutput<V> failover(Queue<V> pool, int batchSize) {
		return new FailoverOutput<V>(this, pool, batchSize);
	}

	default Output<V> batch(int batchSize) {
		return Wrapper.wrap(this, "Batch", items -> batching(items, this::enqueue, batchSize));
	}

	default <K> KeyOutput<K, V> partitial(Function<V, K> partitier, BiConsumer<K, Stream<V>> enqueuing) {
		return new KeyOutput<K, V>() {
			@Override
			public K partition(V v) {
				return partitier.apply(v);
			}

			@Override
			public void enqueue(K key, Stream<V> v) {
				enqueuing.accept(key, v);
			}
		};

	}

	// constructor
	public static <T> Output<T> of(Collection<? super T> underly) {
		return items -> items.forEach(underly::add);
	}

	default void commit() {}
}
