package net.butfly.albatis.io;

import java.util.function.Consumer;

import net.butfly.albacore.io.Dequeuer;
import net.butfly.albacore.io.Enqueuer;
import net.butfly.albacore.lambda.Runnable;
import net.butfly.albacore.paral.steam.Sdream;
import net.butfly.albacore.utils.logger.Logger;

public interface Wrapper {
	static <T, T1> WrapInput<T, T1> wrap(Input<T1> base, String suffix, Dequeuer<T> d) {
		return new WrapInput<T, T1>(base, suffix) {
			@Override
			public void dequeue(Consumer<Sdream<T>> using, int batchSize) {
				d.dequeue(using, batchSize);
			}
		};
	}

	static <T, T1> WrapOutput<T, T1> wrap(Output<T1> base, String suffix, Enqueuer<T> d) {
		return new WrapOutput<T, T1>(base, suffix) {
			@Override
			public void enqueue(Sdream<T> items) {
				d.enqueue(items);
			}

			@Override
			public void succeeded(long c) {
				d.succeeded(c);
			}

			@Override
			public void failed(Sdream<T> failed) {
				d.failed(failed);
			}
		};
	}

	abstract class WrapInput<V, V1> implements Input<V> {
		@Override
		public abstract void dequeue(Consumer<Sdream<V>> using, int batchSize);

		protected final Input<? extends V1> base;
		private String suffix;

		protected WrapInput(Input<? extends V1> origin, String suffix) {
			this.base = origin;
			this.suffix = suffix;
		}

		@Override
		public long capacity() {
			return base.capacity();
		}

		@Override
		public boolean empty() {
			return base.empty();
		}

		@Override
		public boolean full() {
			return base.full();
		}

		@Override
		public Logger logger() {
			return base.logger();
		}

		@Override
		public String name() {
			return base.name() + suffix;
		}

		@Override
		public long size() {
			return base.size();
		}

		@Override
		public String toString() {
			return base.toString() + suffix;
		}

		@Override
		public boolean opened() {
			return base.opened();
		}

		@Override
		public boolean closed() {
			return base.closed();
		}

		@Override
		public void opening(Runnable handler) {
			base.opening(handler);
		}

		@Override
		public void closing(Runnable handler) {
			base.closing(handler);
		}

		@Override
		public void open() {
			base.open();
		}

		@Override
		public void close() {
			base.close();
		}
	}

	abstract class WrapOutput<V, V1> implements Output<V> {
		@Override
		public abstract void enqueue(Sdream<V> items);

		protected final Output<V1> base;
		private String suffix;

		protected WrapOutput(Output<V1> origin, String suffix) {
			this.base = origin;
			this.suffix = suffix;
		}

		@Override
		public long capacity() {
			return base.capacity();
		}

		@Override
		public boolean empty() {
			return base.empty();
		}

		@Override
		public boolean full() {
			return base.full();
		}

		@Override
		public Logger logger() {
			return base.logger();
		}

		@Override
		public String name() {
			return base.name() + suffix;
		}

		@Override
		public long size() {
			return base.size();
		}

		@Override
		public String toString() {
			return base.toString() + suffix;
		}

		@Override
		public boolean opened() {
			return base.opened();
		}

		@Override
		public boolean closed() {
			return base.closed();
		}

		@Override
		public void opening(Runnable handler) {
			base.opening(handler);
		}

		@Override
		public void closing(Runnable handler) {
			base.closing(handler);
		}

		@Override
		public void open() {
			base.open();
		}

		@Override
		public void close() {
			base.close();
		}
	}
}