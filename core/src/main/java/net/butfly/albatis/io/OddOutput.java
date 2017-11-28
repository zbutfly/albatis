package net.butfly.albatis.io;

import static net.butfly.albacore.paral.Task.waitSleep;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.steam.Sdream;

public interface OddOutput<V> extends Output<V> {
	boolean enqueue(V v);

	@Override
	public default void enqueue(Sdream<V> items) {
		if (!waitWhen(() -> full())) return;
		items.eachs(v -> {
			if (enqueue(v)) succeeded(1);
			else failed(v);
		});
	}

	protected abstract boolean enqueue(V v);

	@Override
	public final void enqueue(Sdream<V> s) {
		if (!waitSleep(() -> full())) return;
		s.each(v -> {
			if (enqueue(v)) succeeded(1);
		});
	}

	default void failed(V v) {}
}
