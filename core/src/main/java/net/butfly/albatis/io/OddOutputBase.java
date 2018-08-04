package net.butfly.albatis.io;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.paral.Task;

public abstract class OddOutputBase<V> extends OutputSafeBase<V> implements OddOutput<V> {
	private static final long serialVersionUID = -902415218246097294L;

	protected OddOutputBase(String name) {
		super(name);
	}

	protected abstract boolean enqueue0(V v);

	@Override
	protected final void enqueue0(Sdream<V> items) {
		enqueue(items);
	}

	@Override
	public final void enqueue(Sdream<V> items) {
		while (opExceeded.get())
			Task.waitSleep(100);
		OddOutput.super.enqueue(items);
	}

	@Override
	public final boolean enqueue(V v) {
		opsPending.incrementAndGet();
		try {
			return enqueue0(v);
		} finally {
			opsPending.decrementAndGet();
		}
	}
}
