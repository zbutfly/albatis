package net.butfly.albatis.io;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.paral.Task;

public abstract class SafeOutput<V> extends SafeOutputBase<V> {
	protected SafeOutput(String name) {
		super(name);
	}

	@Override
	protected abstract void enqueue0(Sdream<V> items);

	@Override
	public final void enqueue(Sdream<V> items) {
		while (opExceeded.get())
			Task.waitSleep(100);
		opsPending.incrementAndGet();
		// Exeter.of().execute(() -> {
		try {
			enqueue0(items);
		} finally {
			opsPending.decrementAndGet();
		}
		// });
	}
}
