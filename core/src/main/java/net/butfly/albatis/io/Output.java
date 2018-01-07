package net.butfly.albatis.io;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import net.butfly.albacore.io.Enqueuer;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.ext.FailoverOutput;

public interface Output<V> extends IO, Consumer<Sdream<V>>, Enqueuer<V> {
	static Output<?> NULL = items -> {};

	@Override
	default long size() {
		return 0;
	}

	@Override
	default void accept(Sdream<V> items) {
		enqueue(items);
	}

	default <V0> Output<V0> prior(Function<V0, V> conv) {
		return Wrapper.wrap(this, "Prior", s -> enqueue(s.map(conv)));
	}

	default <V0> Output<V0> priors(Function<Sdream<V0>, Sdream<V>> conv) {
		return priors(conv, 1);
	}

	default <V0> Output<V0> priors(Function<Sdream<V0>, Sdream<V>> conv, int parallelism) {
		return Wrapper.wrap(this, "Priors", (Enqueuer<V0>) s -> s.partition(ss -> enqueue(conv.apply(ss)), parallelism));
	}

	default void commit() {}

	// more extends
	// default Output<V> safe() {
	// return new SafeOutputBase<>(this, detectMaxConcurrentOps(this, logger()));
	// }

	default FailoverOutput<V> failover(Queue<V> pool) {
		return new FailoverOutput<V>(this, pool);
	}

	@Deprecated
	default Output<V> batch(int batchSize) {
		Props.PROPS.computeIfAbsent(this, io -> Maps.of()).put(Props.BATCH_SIZE, batchSize);
		return this;
		// return Wrapper.wrap(this, "Batch", s -> s.batch(this::enqueue, batchSize));
	}

	default <K> KeyOutput<K, V> partitial(Function<V, K> partitier, BiConsumer<K, Sdream<V>> enqueuing) {
		return new KeyOutput<K, V>() {
			@Override
			public K partition(V v) {
				return partitier.apply(v);
			}

			@Override
			public void enqueue(K key, Sdream<V> v) {
				enqueuing.accept(key, v);
			}
		};
	}

	// constructor
	static <T> Output<T> of(Collection<? super T> underly) {
		return s -> s.each(underly::add);
	}
}
