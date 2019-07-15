package net.butfly.albatis.io;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albacore.io.Dequeuer;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.io.lambda.Predicate;
import net.butfly.albacore.io.lambda.Supplier;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.ext.DryOutput;
import net.butfly.albatis.io.ext.FanOutput;
import net.butfly.albatis.io.ext.PrefetchInput;
import net.butfly.albatis.io.pump.BasicPump;
import net.butfly.albatis.io.pump.Pump;

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

	default Input<V> stats() {
		return this;
	}

	static final Map<Input<?>, String> KEY_FIELDS = Maps.of();

	default String keyField() {
		return KEY_FIELDS.get(this);
	}

	default Input<V> keyField(String keyFieldName) {
		KEY_FIELDS.put(this, keyFieldName);
		return this;
	}

	// constructor
	public static <T> Input<T> of(Collection<? extends T> collection, int batchSize) {
		return new Input<T>() {
			private static final long serialVersionUID = 3793970608919903578L;
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
			while (!ending.get() && null != (t = next.get()) && l.size() < batchSize) l.add(t);
			using.accept(Sdream.of(l));
		};
	}

	// atomic methods
	default Input<V> filter(Map<String, Object> criteria) {
		throw new UnsupportedOperationException();
	}

	default Input<V> filter(Predicate<V> predicater) {
		return Wrapper.wrap(this, "Then", using -> dequeue(s -> using.accept(s.filter(predicater))));
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

	// expands
	default PrefetchInput<V> prefetch(Queue<V> pool) {
		return new PrefetchInput<V>(this, pool);
	}

	// pump
	default Pump<V> pump(int parallelism) {
		return pump(parallelism, new DryOutput<V>(name()));
	}

	default Pump<V> pump(int parallelism, Output<V> dest) {
		return new BasicPump<V>(this, parallelism, dest);
	}

	default Pump<V> pump(int parallelism, @SuppressWarnings("unchecked") Output<V>... dests) {
		return pump(parallelism, Arrays.asList(dests));
	}

	default Pump<V> pump(int parallelism, List<? extends Output<V>> dests) {
		return pump(parallelism, dests, true);
	}

	default Pump<V> pump(int parallelism, List<? extends Output<V>> dests, boolean paral) {
		return pump(parallelism, new FanOutput<V>(dests, paral));
	}
}
