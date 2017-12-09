package net.butfly.albatis.io;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

import net.butfly.albacore.paral.Sdream;

public interface OddInput<V> extends Input<V> {
	V dequeue();

	@Override
	public default void dequeue(Consumer<Sdream<V>> using, int batchSize) {
		using.accept(Sdream.of(new SupSpliterator<>(this::dequeue, () -> empty() || !opened(), batchSize)));
	}

	class SupSpliterator<V> implements Spliterator<V> {
		private int est;
		private Supplier<V> get;
		private Supplier<Boolean> end;

		public SupSpliterator(Supplier<V> get, Supplier<Boolean> end, int limit) {
			super();
			this.est = limit;
			this.get = get;
			this.end = end;
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
		public boolean tryAdvance(Consumer<? super V> using1) {
			V v = null;
			while (est > 0 && !end.get() && null == (v = get.get())) {}
			if (null == v) {
				est = 0;
				return false;
			} else {
				est--;
				using1.accept(v);
				return true;
			}
		}

		@Override
		public Spliterator<V> trySplit() {
			// TODO not split now
			return null;
		}
	}
}
