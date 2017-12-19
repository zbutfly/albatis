package net.butfly.albatis.io;

import java.util.concurrent.atomic.AtomicInteger;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.paral.Task;

public abstract class SafeOutput<V> extends SafeOutputBase<V> {
	protected SafeOutput(String name) {
		super(name);
	}

	@Override
	protected abstract void enqueue(Sdream<V> items, AtomicInteger ops);

	@Override
	public final void enqueue(Sdream<V> items) {
		while (opExceeded.get())
			Task.waitSleep(100);
		enqueue(items, currOps);
	}
}
