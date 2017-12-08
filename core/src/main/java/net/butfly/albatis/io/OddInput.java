package net.butfly.albatis.io;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;

public interface OddInput<V> extends Input<V> {
	final static long TIMEOUT_MS = 1000;

	V dequeue();

	@Override
	public default void dequeue(Consumer<Sdream<V>> using, int batchSize) {
		List<V> batch = Colls.list();
		try {
			while (!empty() && opened()) {
				while (batch.size() < batchSize && !empty()) {
					Future<V> f = Exeter.of().submit((Callable<V>) this::dequeue);
					V v;
					try {
						v = f.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
					} catch (ExecutionException e) {
						logger().error("Dequeue fail", e);
						return;
					} catch (InterruptedException e) {
						logger().warn("Dequeue interrupted");
						return;
					} catch (TimeoutException e) {
						return;
					}
					if (null != v) batch.add(v);
				}
			}
		} finally {
			if (batch.isEmpty()) using.accept(Sdream.of(batch));
		}
	}
}
