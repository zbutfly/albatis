package net.butfly.albatis.io;

import static net.butfly.albacore.paral.Task.waitWhen;

import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicLong;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Supplier;
import net.butfly.albacore.paral.Sdream;

public abstract class OddQueue<T> extends Namedly implements Queue<T> {
	private static final long serialVersionUID = -8930489494229450548L;
	private final AtomicLong capacity;

	public abstract T dequeue();

	public abstract boolean enqueue(T v);

	public void deq(Consumer<T> using) {
		using.accept(dequeue());
	}

	public void failed(T v) {}

	@Override
	public void enqueue(Sdream<T> items) {
		if (!waitWhen(() -> full())) return;
		items.eachs(v -> {
			if (enqueue(v)) succeeded(1);
			else failed(v);
		});
	}

	@Override
	public void failed(Sdream<T> fails) {
		fails.eachs(this::failed);
	}

	@Override
	public void dequeue(Consumer<Sdream<T>> using) {
		SupSpliterator<T> s = new SupSpliterator<>(this, () -> empty() || !opened(), batchSize());
		using.accept(Sdream.of(s));
	}

	// ====
	protected OddQueue(String name, long capacity) {
		super(name);
		this.capacity = new AtomicLong(capacity);
	}

	@Override
	public final long capacity() {
		return capacity.get();
	}

	@Override
	public String toString() {
		return name() + "[" + size() + "]";
	}

	// ==
	class SupSpliterator<V> implements Spliterator<V> {
		private int est;
		private final OddQueue<V> input;

		public SupSpliterator(OddQueue<V> get, Supplier<Boolean> end, int limit) {
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

	@Override
	public int features() {
		return Queue.super.features() | IO.Feature.ODD;
	}
}
