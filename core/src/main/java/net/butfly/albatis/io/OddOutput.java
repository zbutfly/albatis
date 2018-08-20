package net.butfly.albatis.io;

import static net.butfly.albacore.paral.Task.waitWhen;

import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Sdream;

public interface OddOutput<V> extends Output<V> {
	boolean enqueue(V v);

	default void failed(V v) {}

	@Override
	default void enqueue(Sdream<V> items) {
		if (!waitWhen(() -> full())) return;
		items.eachs(v -> {
			if (enqueue(v)) succeeded(1);
			else failed(v);
		});
	}

	@Override
	default void failed(Sdream<V> fails) {
		fails.eachs(this::failed);
	}

	// ===
	@Override
	default <V0> Output<V0> prior(Function<V0, V> conv) {
		return Wrapper.wrapOdd(this, "Prior", t -> {
			if (null == t) return false;
			V t0 = conv.apply(t);
			return null == t0 ? false : enqueue(t0);
		});
	}

	@Override
	default <V0> Output<V0> priors(Function<Sdream<V0>, Sdream<V>> conv) {
		return Wrapper.wrap(this, "Prior", s0 -> conv.apply(s0).eachs(v -> {
			if (null != v) enqueue(v);
		}));
	}

	@Override
	@Deprecated
	default <V0> Output<V0> priors(Function<Sdream<V0>, Sdream<V>> conv, int parallelism) {
		return priors(conv);
	}

	@Override
	default <V0> Output<V0> priorFlat(Function<V0, Sdream<V>> conv) {
		return Wrapper.wrap(this, "PriorFlat", s -> s.mapFlat(conv).eachs(v -> {
			if (null != v) enqueue(v);
		}));
	}

	@Override
	default int features() {
		return Output.super.features() | IO.Feature.ODD;
	}
}
