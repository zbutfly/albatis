package net.butfly.albatis.io;

import static net.butfly.albacore.paral.Task.waitWhen;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.paral.Task;

public abstract class SafeOddOutput<V> extends SafeOutputBase<V> implements OddOutput<V> {
	protected SafeOddOutput(String name) {
		super(name);
	}

	protected abstract boolean enqSafe(V v);

	@Override
	public final void enqueue(Sdream<V> items) {
		while (opExceeded.get())
			Task.waitSleep(100);
		enqSafe(items);
	}

	@Override
	public boolean enqueue(V v) {
		return enqSafe(v);
	}

	@Override
	protected final void enqSafe(Sdream<V> items) {
		if (!waitWhen(() -> full())) return;
		items.eachs(v -> {
			if (enqSafe(v)) succeeded(1);
		});
	}
}
