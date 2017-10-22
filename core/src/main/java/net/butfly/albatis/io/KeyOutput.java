package net.butfly.albatis.io;

import static net.butfly.albacore.utils.collection.Streams.batching;
import static net.butfly.albacore.utils.collection.Streams.collect;
import static net.butfly.albacore.utils.collection.Streams.of;
import static net.butfly.albacore.utils.parallel.Parals.eachs;

import java.util.List;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albatis.io.ext.FailoverOutput;

public interface KeyOutput<K, V> extends Output<V> {
	K partition(V v);

	void enqueue(K key, Stream<V> v);

	@Override
	public default void enqueue(Stream<V> s) {
		eachs(collect(s, Collectors.groupingBy(v -> partition(v))).entrySet(), //
				(Consumer<Entry<K, List<V>>>) e -> enqueue(e.getKey(), of(e.getValue())));
	}

	@Override
	default FailoverKeyOutput<K, V> failover(Queue<V> pool, int batchSize) {
		return new FailoverKeyOutput<K, V>(this, pool, batchSize);
	}

	@Override
	default KeyOutput<K, V> batch(int batchSize) {
		KeyOutput<K, V> origin = this;
		KeyOutput<K, V> ko = new KeyOutput<K, V>() {
			@Override
			public K partition(V v) {
				return origin.partition(v);
			}

			@Override
			public void enqueue(K key, Stream<V> v) {
				batching(v, s -> enqueue(key, s), batchSize);
			}
		};
		ko.opening(() -> origin.open());
		ko.closing(() -> origin.close());
		return ko;
	}

	default void commit(K key) {
		commit();
	}

	@SuppressWarnings("unchecked")
	class FailoverKeyOutput<K, V> extends FailoverOutput<V> implements KeyOutput<K, V> {
		public FailoverKeyOutput(KeyOutput<K, V> output, Queue<V> failover, int batchSize) {
			super(output, failover, batchSize);
		}

		@Override
		public K partition(V v) {
			return ((KeyOutput<K, V>) base).partition(v);
		}

		@Override
		public void enqueue(K key, Stream<V> v) {
			((KeyOutput<K, V>) base).enqueue(key, v);
		}
	}
}
