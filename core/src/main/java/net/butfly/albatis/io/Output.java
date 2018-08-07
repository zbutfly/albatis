package net.butfly.albatis.io;

import java.util.Collection;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;

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
		return Wrapper.wrap(this, "Prior", s -> enqueue(conv.apply(s)));
	}

	@Deprecated
	default <V0> Output<V0> priors(Function<Sdream<V0>, Sdream<V>> conv, int parallelism) {
		return Wrapper.wrap(this, "Priors", s -> s.partition(ss -> enqueue(conv.apply(ss)), parallelism));
	}

	default <V0> Output<V0> priorFlat(Function<V0, Sdream<V>> conv) {
		return Wrapper.wrap(this, "PriorFlat", s -> enqueue(s.mapFlat(conv)));
	}

	default void commit() {}

	default FailoverOutput<V> failover(Queue<V> pool) {
		return new FailoverOutput<V>(this, pool);
	}

	@Deprecated
	default Output<V> batch(int batchSize) {
		Props.PROPS.computeIfAbsent(this, io -> Maps.of()).put(Props.BATCH_SIZE, batchSize);
		return this;
	}

	static <T> Output<T> of(Collection<? super T> underly) {
		return s -> s.eachs(underly::add);
	}
}
