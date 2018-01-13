package net.butfly.albatis.io;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.paral.Task;
import net.butfly.albacore.utils.collection.Colls;

public abstract class OutputBase<V> extends OutputSafeBase<V> {
	private BlockingQueue<V> batchPool = new LinkedBlockingQueue<>();

	protected OutputBase(String name) {
		super(name);
	}

	@Override
	protected abstract void enqueue0(Sdream<V> items);

	@Override
	public final void enqueue(Sdream<V> items) {
		while (opExceeded.get())
			Task.waitSleep(100);
		opsPending.incrementAndGet();
		try {
			List<Future<?>> fs = Colls.list();
			items.eachs(batchPool::add);
			while (!batchPool.isEmpty()) {
				List<V> batch = Colls.list();
				batchPool.drainTo(batch, BATCH_SIZE);
				fs.add(Exeter.of().submit(() -> enqueue0(Sdream.of(batch))));
			}
			Exeter.getn(fs);
		} finally {
			opsPending.decrementAndGet();
		}
	}
}
