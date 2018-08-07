package net.butfly.albatis.io;

import java.util.Spliterator;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.io.lambda.Predicate;
import net.butfly.albacore.io.lambda.Supplier;

import net.butfly.albacore.io.Dequeuer;
import net.butfly.albacore.paral.Sdream;

public interface OddInput<V> extends Input<V> {
	V dequeue();

	default void deq(Consumer<V> using) {
		using.accept(dequeue());
	}

	@Override
	default void dequeue(Consumer<Sdream<V>> using) {
		using.accept(Sdream.of(new SupSpliterator<>(this, () -> empty() || !opened(), batchSize())));
	}

	class SupSpliterator<V> implements Spliterator<V> {
		private int est;
		private final OddInput<V> input;

		public SupSpliterator(OddInput<V> get, Supplier<Boolean> end, int limit) {
			super();
			this.est = limit;
			this.input = get;
			// this.end = end;
		}

		@Override
		public int characteristics() {
			return CONCURRENT | SIZED | SUBSIZED | IMMUTABLE | NONNULL;
		}

		@Override
		public long estimateSize() {
			return est;
		}

		@Override
		public boolean tryAdvance(java.util.function.Consumer<? super V> using1) {
			V v = null;
			while (input.opened() && !input.empty() && est > 0) {
				v = input.dequeue();
				if (null != v) {
					est--;
					using1.accept(v);
					return true;
				} else break;
			}
			est = 0;
			return false;
		}

		@Override
		public Spliterator<V> trySplit() {
			// TODO not split now
			return null;
		}
	}

	// =====
	@Override
	default OddInput<V> filter(Predicate<V> predicater) {
		return Wrapper.wrapOdd(this, "Then", () -> {
			V v = predicater.test(v = dequeue()) ? v : null;
			return v;
		});
	}

	@Override
	default <V1> OddInput<V1> then(Function<V, V1> conv) {
		return Wrapper.wrapOdd(this, "Then", () -> {
			V v = dequeue();
			return null == v ? null : conv.apply(v);
		});
	}

	@Override
	default <V1> Input<V1> thens(Function<Sdream<V>, Sdream<V1>> conv) {
		return Wrapper.wrap(this, "Thens", (Dequeuer<V1>) using -> {
			V v = dequeue();
			if (null != v) using.accept(conv.apply(Sdream.of(v)));
		});
	}

	@Override
	@Deprecated
	default <V1> Input<V1> thens(Function<Sdream<V>, Sdream<V1>> conv, int parallelism) {
		return thens(conv);
	}

	@Override
	default <V1> Input<V1> thenFlat(Function<V, Sdream<V1>> conv) {
		return Wrapper.wrap(this, "ThenFlat", (Dequeuer<V1>) using -> {
			V v = dequeue();
			if (null != v) dequeue(s -> using.accept(conv.apply(v)));
		});
	}

	@Override
	default int features() {
		return Input.super.features() | IO.Feature.ODD;
	}
}
