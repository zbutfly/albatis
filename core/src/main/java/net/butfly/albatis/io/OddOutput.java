package net.butfly.albatis.io;

import static net.butfly.albacore.paral.Task.waitWhen;

import net.butfly.albacore.paral.Sdream;

public interface OddOutput<V> extends Output<V> {
	boolean enqueue(V v);

	@Override
	public default void enqueue(Sdream<V> s) {
		if (!waitWhen(() -> full())) return;
		s.eachs(v -> {
			if (enqueue(v)) succeeded(1);
		});
	}
}
