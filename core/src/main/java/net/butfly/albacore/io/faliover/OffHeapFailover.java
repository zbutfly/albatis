package net.butfly.albacore.io.faliover;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.Message;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.async.Concurrents;

public class OffHeapFailover<K, V extends Message<K, ?, V>> extends Failover<K, V> {
	private IBigQueue failover;

	public OffHeapFailover(String parentName, FailoverOutput<K, V> output, Function<byte[], ? extends V> constructor, String path,
			String poolName) throws IOException {
		super(parentName, output, constructor);
		if (poolName == null) poolName = "POOL";
		failover = new BigQueueImpl(IOs.mkdirs(path + "/" + parentName), poolName);
		logger.info(MessageFormat.format("Failover [persist mode] init: [{0}/{1}] with name [{2}], init size [{3}].", //
				path, parentName, poolName, size()));
	}

	@Override
	protected void exec() {
		Map<K, List<V>> fails = new HashMap<>();
		while (opened()) {
			while (opened() && failover.isEmpty())
				Concurrents.waitSleep(1000);
			fails.clear();
			while (opened() && !failover.isEmpty()) {
				byte[] buf;
				try {
					buf = failover.dequeue();
				} catch (IOException e) {
					logger.error("invalid failover found and lost.");
					continue;
				}
				if (null == buf) return;
				V value;
				try {
					value = construct.apply(buf);
				} catch (Throwable t) {
					logger().error("Failover data invalid on loading and lost", Exceptions.unwrap(t));
					continue;
				}
				K key = value.partition();
				List<V> l = fails.computeIfAbsent(key, c -> new ArrayList<>(output.packageSize));
				l.add(value);
				if (l.size() >= output.packageSize) output(key, fails.remove(key));
			}
			for (K key : fails.keySet())
				output(key, fails.remove(key));
		}
	}

	@Override
	public long size() {
		return failover.size();
	}

	@Override
	public boolean isEmpty() {
		return failover.isEmpty();
	}

	@Override
	public int fail(K key, Collection<V> values, Exception err) {
		AtomicInteger c = new AtomicInteger();
		IO.each(values, v -> {
			try {
				failover.enqueue(v.toBytes());
				c.incrementAndGet();
			} catch (IOException e) {
				if (null != err) logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}], original caused by [{2}]", //
						values.size(), key, err.getMessage()), e);
				else logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}]", values.size(), key), e);
			}
		});
		if (null != err) logger.warn(MessageFormat.format(
				"Failure added on [{0}] with [{1}] docs, now [{2}] failover on [{0}], caused by [{3}]", //
				key, values.size(), size(), err.getMessage()));
		return c.get();
	}

	@Override
	public void close(Runnable run) {
		super.close(this::closePool);
	}

	private void closePool() {
		try {
			failover.gc();
		} catch (IOException e) {
			logger.error("Failover cleanup failure", e);
		}
		long size = size();
		try {
			failover.close();
		} catch (IOException e) {
			logger.error("Failover close failure", e);
		} finally {
			if (size > 0) logger.error("Failover closed and remained [" + size + "].");
		}
	}
}