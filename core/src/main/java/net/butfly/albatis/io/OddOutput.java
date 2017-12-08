package net.butfly.albatis.io;

import static net.butfly.albacore.paral.Task.waitSleep;

import net.butfly.albacore.paral.Sdream;

public interface OddOutput<V> extends Output<V> {
	boolean enqueue(V v);

	@Override
	public default void enqueue(Sdream<V> s) {
		if (!waitSleep(() -> full())) return;
		s.each(v -> {
			if (enqueue(v)) succeeded(1);
		});
	}
}
