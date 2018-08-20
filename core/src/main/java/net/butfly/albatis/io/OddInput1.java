package net.butfly.albatis.io;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;

public interface OddInput1<V> extends Input<V> {
	final static long TIMEOUT_MS = 1000;

	V dequeue();

	@Override
	public default void dequeue(Consumer<Sdream<V>> using) {
		List<V> batch = Colls.list();
		try {
			while (!empty() && opened() && batch.size() < batchSize()) {
				Future<V> f = Exeter.of().submit((Callable<V>) this::dequeue);
				V v = null;
				try {
					v = f.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
				} catch (ExecutionException e) {
					logger().error("Dequeue fail", e);
					return;
				} catch (InterruptedException e) {
					logger().warn("Dequeue interrupted");
					return;
				} catch (TimeoutException e) {
					if (f.isDone()) try {
						v = f.get();
					} catch (ExecutionException ee) {
						logger().error("Dequeue fail", e);
						return;
					} catch (InterruptedException ee) {
						logger().warn("Dequeue interrupted");
						return;
					}
					else {
						f.cancel(true);
						logger().debug("Dequeue timeout, finish current batch and continue for next batch");
						v = null;
					}
				}
				if (null == v && !batch.isEmpty()) return;
				if (null != v) batch.add(v);
			}
		} finally {
			if (!batch.isEmpty()) //
				using.accept(Sdream.of(batch));
		}
	}

	@Override
	default int features() {
		return Input.super.features() | IO.Feature.ODD;
	}
}
