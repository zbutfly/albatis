package net.butfly.albatis.io;

import static net.butfly.albacore.paral.Task.waitWhen;

import java.util.concurrent.atomic.AtomicInteger;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.paral.Task;

public abstract class SafeOddOutput<V> extends SafeOutputBase<V> implements OddOutput<V> {
	protected SafeOddOutput(String name) {
		super(name);
	}

	protected abstract boolean enqueue1(V v, AtomicInteger ops);

	@Override
	public final void enqueue(Sdream<V> items) {
		while (opExceeded.get())
			Task.waitSleep(100);
		enqueue(items, currOps);
	}

	@Override
	public boolean enqueue(V v) {
		return enqueue1(v, currOps);
	}

	@Override
	protected final void enqueue(Sdream<V> items, AtomicInteger ops) {
		if (!waitWhen(() -> full())) return;
		items.eachs(v -> {
			if (enqueue1(v, ops)) succeeded(1);
		});
	}
}
