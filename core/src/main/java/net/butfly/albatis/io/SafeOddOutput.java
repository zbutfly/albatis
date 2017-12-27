package net.butfly.albatis.io;

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
		opsPending.incrementAndGet();
		return enqSafe(v);
	}

	@Override
	protected final void enqSafe(Sdream<V> items) {
		items.eachs(v -> {
			opsPending.incrementAndGet();
			if (enqSafe(v)) succeeded(1);
		});
	}
}
