package net.butfly.albacore.io.faliover;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import net.butfly.albacore.lambda.ConverterPair;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.async.Concurrents;
import scala.Tuple2;

public abstract class OffHeapFailover<K, V> extends Failover<K, V> {
	private static final long serialVersionUID = -4766585003300311051L;
	private IBigQueue failover;

	public OffHeapFailover(String parentName, ConverterPair<K, List<V>, Exception> writing, Consumer<K> committing, String path,
			String poolName, int packageSize, int parallelism) throws IOException {
		super(parentName, writing, committing, packageSize, parallelism);
		if (poolName == null) poolName = "POOL";
		failover = new BigQueueImpl(IOs.mkdirs(path + "/" + parentName), poolName);
		start();
		logger.info(MessageFormat.format("Failover [persist mode] init: [{0}/{1}] with name [{2}], init size [{3}].", //
				path, parentName, poolName, size()));
	}

	protected abstract byte[] toBytes(K key, V value);

	protected abstract Tuple2<K, V> fromBytes(byte[] bytes);

	@Override
	protected void retry() {
		while (opened() && failover.isEmpty())
			Concurrents.waitSleep(1000);
		Map<K, List<V>> fails = new HashMap<>();
		while (opened() && !failover.isEmpty()) {
			byte[] buf;
			try {
				buf = failover.dequeue();
			} catch (IOException e) {
				continue;
			}
			if (null == buf) return;
			Tuple2<K, V> sm = fromBytes(buf);
			if (null == sm) {
				logger.error("invalid failover found and lost.");
				continue;
			}
			List<V> l = fails.computeIfAbsent(sm._1, c -> new ArrayList<>(packageSize));
			l.add(sm._2);
			if (l.size() < packageSize && !failover.isEmpty()) continue;
			l = fails.remove(sm._1);
			stats(l);
			Exception e = writing.apply(sm._1, l);
			if (null != e) fail(sm._1, l, e);

		}
		for (K core : fails.keySet()) {
			List<V> l = fails.remove(core);
			Exception e = writing.apply(core, l);
			if (null != e) fail(core, l, e);
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
	public int fail(K core, List<V> docs, Exception err) {
		int c = 0;
		for (V doc : docs)
			try {
				failover.enqueue(toBytes(core, doc));
				c++;
			} catch (IOException e) {
				if (null != err) logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}], original caused by [{2}]", //
						docs.size(), core, err.getMessage()), e);
				else logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}]", docs.size(), core), e);
			}
		if (null != err) logger.warn(MessageFormat.format(
				"Failure added on [{0}] with [{1}] docs, now [{2}] failover on [{0}], caused by [{3}]", //
				core, docs.size(), size(), err.getMessage()));
		return c;
	}

	@Override
	public void close() {
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