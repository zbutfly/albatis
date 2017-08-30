package net.butfly.albatis.io;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import net.butfly.albacore.io.Enqueue;
import net.butfly.albacore.io.EnqueueException;
import net.butfly.albacore.io.IO;
import net.butfly.albacore.utils.collection.Its;
import net.butfly.albacore.utils.collection.Streams;
import net.butfly.albacore.utils.parallel.Parals;

public interface Output<V> extends IO, Consumer<Stream<V>>, Enqueue<V> {
	static Output<?> NULL = items -> 0;

	@Override
	default long size() {
		return 0;
	}

	@Override
	default void accept(Stream<V> items) {
		enqueue(items);
	}

	default <V0> Output<V0> prior(Function<V0, V> conv) {
		return Wrapper.wrap(this, items -> enqueue(Streams.of(items.map(conv))));
	}

	default <V0> Output<V0> priors(Function<Iterable<V0>, Iterable<V>> conv, int parallelism) {
		return Wrapper.wrap(this, items -> Parals.eachs(Streams.spatial(items, parallelism).values(), s0 -> enqueue(Streams.of(conv.apply(
				(Iterable<V0>) () -> Its.it(s0)))), Streams.LONG_SUM));
	}

	// more extends

	default Output<V> failover(Queue<V> pool) {
		return Wrapper.wrap(this, items -> {
			try {
				return enqueue(items);
			} catch (EnqueueException ex) {
				pool.enqueue(Streams.of(ex.fails()));
				return ex.success();
			}
		});
	}

	default Output<V> batch(int batchSize) {
		return Wrapper.wrap(this, items -> {
			AtomicLong c = new AtomicLong();
			Stream<Stream<V>> ss = Streams.batching(items, batchSize);
			ss.parallel().forEach(s -> c.addAndGet(enqueue(s)));
			return c.get();
		});
	}

	default <K> KeyOutput<K, V> partitial(Function<V, K> partitier, BiFunction<K, Stream<V>, Long> enqueuing) {
		return new KeyOutput<K, V>() {
			@Override
			protected K partitionize(V v) {
				return partitier.apply(v);
			}

			@Override
			public long enqueue(K key, Stream<V> v) throws EnqueueException {
				return enqueuing.apply(key, v);
			}
		};

	}

	// constructor
	public static <T> Output<T> of(Collection<? super T> underly) {
		return items -> {
			items.forEach(underly::add);
			return underly.size();
		};

	}

	default void commit() {}
}
