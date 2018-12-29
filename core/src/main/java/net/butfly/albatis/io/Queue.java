package net.butfly.albatis.io;

import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Sdream;

public interface Queue<T> extends Queue0<T, T> {
	@Override
	public default <O1> Queue0<T, O1> then(Function<T, O1> conv) {
		return Queue0.super.then(conv);
	}

	@Override
	public default <O1> Queue0<T, O1> thenFlat(Function<T, Sdream<O1>> conv) {
		return Queue0.super.thenFlat(conv);
	}

	@Override
	public default <I0> Queue0<I0, T> prior(Function<I0, T> conv) {
		return Queue0.super.prior(conv);
	}

	@Override
	public default <I0> Queue0<I0, T> priorFlat(Function<I0, Sdream<T>> conv) {
		return Queue0.super.priorFlat(conv);
	}
}
