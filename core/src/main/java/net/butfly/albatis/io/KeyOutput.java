package net.butfly.albatis.io;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.EnqueueException;
import net.butfly.albacore.utils.collection.Streams;
import net.butfly.albacore.utils.parallel.Parals;

public abstract class KeyOutput<K, V> extends Namedly implements Output<V> {
	public KeyOutput() {
		super();
	}

	public KeyOutput(String name) {
		super(name);
	}

	protected abstract K partitionize(V v);

	public abstract long enqueue(K key, Stream<V> v) throws EnqueueException;

	@Override
	public long enqueue(Stream<V> items) throws EnqueueException {
		Set<Entry<K, List<V>>> m = items.collect(Collectors.groupingBy(v -> partitionize(v))).entrySet();
		AtomicLong c = new AtomicLong();
		Parals.eachs(m, e -> c.addAndGet(enqueue(e.getKey(), Streams.of(e.getValue()))));
		return c.get();
	}

	@Override
	public KeyOutput<K, V> failover(Queue<V> pool) {
		KeyOutput<K, V> origin = this;
		KeyOutput<K, V> ko = new KeyOutput<K, V>() {
			@Override
			protected K partitionize(V v) {
				return origin.partitionize(v);
			}

			@Override
			public long enqueue(K key, Stream<V> v) throws EnqueueException {
				try {
					return origin.enqueue(key, v);
				} catch (EnqueueException ex) {
					pool.enqueue(Streams.of(ex.fails()));
					return ex.success();
				}
			}
		};
		ko.opening(() -> origin.open());
		ko.closing(() -> origin.close());
		return ko;
	}

	@Override
	public KeyOutput<K, V> batch(int batchSize) {
		KeyOutput<K, V> origin = this;
		KeyOutput<K, V> ko = new KeyOutput<K, V>() {
			@Override
			protected K partitionize(V v) {
				return origin.partitionize(v);
			}

			@Override
			public long enqueue(K key, Stream<V> v) throws EnqueueException {
				AtomicLong c = new AtomicLong();
				Stream<Stream<V>> ss = Streams.batching(v, batchSize);
				ss.parallel().forEach(s -> c.addAndGet(enqueue(key, s)));
				return c.get();
			}
		};
		ko.opening(() -> origin.open());
		ko.closing(() -> origin.close());
		return ko;
	}

	public void commit(K key) {
		commit();
	}
}
