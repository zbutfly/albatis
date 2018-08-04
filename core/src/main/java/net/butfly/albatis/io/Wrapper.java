package net.butfly.albatis.io;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import net.butfly.albacore.base.Named;
import net.butfly.albacore.io.Dequeuer;
import net.butfly.albacore.paral.Sdream;

public interface Wrapper<B extends IO> extends IO, Named {
	B bases();

	static <T, T1> WrapInput<T, T1> wrap(Input<T1> base, String suffix, Dequeuer<T> d) {
		return new WrapInput<T, T1>(base, suffix) {
			@Override
			public void dequeue(Consumer<Sdream<T>> using) {
				d.dequeue(using);
			}
		};
	}

	static <T0, T> WrapOutput<T0, T> wrap(Output<T> base, String suffix, Consumer<Sdream<T0>> d) {
		return new WrapOutput<T0, T>(base, suffix) {
			@Override
			public void enqueue(Sdream<T0> items) {
				d.accept(items);
			}

			@Override
			public void succeeded(long c) {
				base.succeeded(c);
			}

			@Override
			public void failed(Sdream<T0> failed) {
				logger().error("Wrapper could not failed, the unconv not provided.");
			}
		};
	}

	static <T, T1> WrapOddInput<T, T1> wrapOdd(OddInput<T1> base, String suffix, Supplier<T> d) {
		return new WrapOddInput<T, T1>(base, suffix) {
			@Override
			public T dequeue() {
				return d.get();
			}
		};
	}

	static <T0, T> WrapOddOutput<T0, T> wrapOdd(OddOutput<T> base, String suffix, Function<T0, Boolean> d) {
		return new WrapOddOutput<T0, T>(base, suffix) {
			@Override
			public boolean enqueue(T0 item) {
				return d.apply(item);
			}

			@Override
			public void succeeded(long c) {
				base.succeeded(c);
			}

			@Override
			public void failed(Sdream<T0> failed) {
				logger().error("Wrapper could not failed, the unconv not provided.");
			}
		};
	}
}