package net.butfly.albacore.io.faliover;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import net.butfly.albacore.io.Message;

public class HeapFailover<K, V extends Message<K, ?, V>> extends Failover<K, V> {
	private static final int MAX_FAILOVER = 50000;
	private Map<K, LinkedBlockingQueue<V>> failover = new ConcurrentHashMap<>();

	public HeapFailover(String parentName, FailoverOutput<K, V> output, Function<byte[], ? extends V> constructor) throws IOException {
		super(parentName, output, constructor);
		failover = new ConcurrentHashMap<>();
		logger.info(MessageFormat.format("SolrOutput [{0}] failover [memory mode] init.", parentName));
	}

	@Override
	public long size() {
		long c = 0;
		for (K core : failover.keySet())
			c += failover.get(core).size();
		return c;
	}

	@Override
	public boolean isEmpty() {
		return failover.isEmpty();
	}

	@Override
	public int fail(K core, Collection<V> docs, Exception err) {
		try {
			failover.computeIfAbsent(core, k -> new LinkedBlockingQueue<>(MAX_FAILOVER)).addAll(docs);
			if (null != err) logger.warn(MessageFormat.format(
					"Failure added on [{0}] with [{1}] docs, now [{2}] failover on [{0}], caused by [{3}]", //
					core, docs.size(), size(), err.getMessage()));
			return docs.size();
		} catch (IllegalStateException ee) {
			if (null != err) logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}], original caused by [{2}]", //
					docs.size(), core, err.getMessage()));
			else logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}]", docs.size(), core));
			return 0;
		}
	}

	@Override
	protected void exec() {
		List<V> retries = new ArrayList<>(output.packageSize);
		while (opened()) {
			for (K key : failover.keySet()) {
				LinkedBlockingQueue<V> fails = failover.get(key);
				retries.clear();
				fails.drainTo(retries, output.packageSize);
				if (!retries.isEmpty()) output(key, retries);
			}
		}
	}
}