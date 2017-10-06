package net.butfly.albatis.io;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

import net.butfly.albacore.paral.Sdream;

public interface OddInput<V> extends Input<V> {
	V dequeue();

	@Override
	public default void dequeue(Consumer<Sdream<V>> using) {
		using.accept(Sdream.of(new SupSpliterator<>(this, () -> empty() || !opened(), batchSize())));
	}

	class SupSpliterator<V> implements Spliterator<V> {
		private int est;
		private final OddInput<V> input;

	@Override
	public final void dequeue(Consumer<Stream<V>> using, int batchSize) {
		using.accept(Streams.of(this::dequeue, batchSize, () -> empty() && opened()).filter(Streams.NOT_NULL));
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
}
