package net.butfly.albatis.io;

import java.util.function.Function;
import java.util.stream.Stream;

import net.butfly.albacore.io.Dequeue;
import net.butfly.albacore.io.Enqueue;
import net.butfly.albacore.lambda.Runnable;
import net.butfly.albacore.utils.logger.Logger;

public interface Wrapper {
	static <T, T1> WrapInput<T, T1> wrap(Input<T1> base, String suffix, Dequeue<T> d) {
		return new WrapInput<T, T1>(base, suffix) {
			@Override
			public long dequeue(Function<Stream<T>, Long> using, int batchSize) {
				return d.dequeue(using, batchSize);
			}
		};
	}

	static <T, T1> WrapOutput<T, T1> wrap(Output<T1> base, String suffix, Enqueue<T> d) {
		return new WrapOutput<T, T1>(base, suffix) {
			@Override
			public long enqueue(Stream<T> items) {
				return d.enqueue(items);
			}
		};
	}

	abstract class WrapInput<V, V1> implements Input<V> {
		@Override
		public abstract long dequeue(Function<Stream<V>, Long> using, int batchSize);

		protected final Input<V1> base;
		private String suffix;

		protected WrapInput(Input<V1> origin, String suffix) {
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
		public abstract long enqueue(Stream<V> items);

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