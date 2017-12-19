package net.butfly.albatis.io;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.paral.Task;
import net.butfly.albatis.io.Wrapper.WrapOutput;

public class SafeOutput<V> extends WrapOutput<V, V> {
	private final Supplier<Boolean> opExceeded;
	private final AtomicInteger currOps;

	protected SafeOutput(Output<V> origin, int maxOps) {
		super(origin, "Safe");
		this.currOps = new AtomicInteger(0);
		this.opExceeded = maxOps > 0 ? () -> currOps.get() > maxOps : () -> false;
	}

	@Override
	public void enqueue(Sdream<V> items) {
		currOps.incrementAndGet();
		while (opExceeded.get())
			Task.waitSleep(100);
		try {
			base.enqueue(items);
		} finally {
			currOps.decrementAndGet();
		}
	}

	@Override
	public void close() {
		int remained;
		int waited = 1;
		while (0 < (remained = currOps.get())) {
			int r = remained;
			logger().info("Output ops [" + r + "] remained, waiting " + (waited++) + " second for safe closing.");
			if (!Task.waitSleep(1000)) break;
			// Task.waitSleep(1000, logger(), () -> "Output ops [" + r + "] remained, waiting for safe closing until all finishing.");
		}
		super.close();
	}

	@Override
	public String toString() {
		return super.toString() + "[Pending Ops: " + currOps.get() + "]";
	}
}
