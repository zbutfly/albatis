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
	public default long enqueue(Stream<V> s) throws EnqueueException {
		Set<Entry<K, List<V>>> m = s.filter(Streams.NOT_NULL).collect(Collectors.groupingBy(v -> partition(v))).entrySet();
		AtomicLong c = new AtomicLong();
		Parals.eachs(m, e -> c.addAndGet(enqueue(e.getKey(), Streams.of(e.getValue()))));
		return c.get();
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
		public long enqueue(K key, Stream<V> v) throws EnqueueException {
			try {
				return ((KeyOutput<K, V>) base).enqueue(key, v);
			} catch (EnqueueException ex) {
				pool.enqueue(of(ex.fails()));
				return ex.success();
			}
		}
	}
}
