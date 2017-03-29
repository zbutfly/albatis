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

import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.Message;
import net.butfly.albacore.io.utils.Streams;

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
	public long fail(K core, Collection<V> docs) {
		try {
			failover.computeIfAbsent(core, k -> new LinkedBlockingQueue<>(MAX_FAILOVER)).addAll(docs);
			return docs.size();
		} catch (IllegalStateException ee) {
			logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}]", docs.size(), core));
			return 0;
		}
	}

	@Override
	protected void exec() {
		List<V> retries = new ArrayList<>(output.batchSize);
		while (opened()) {
			for (K key : failover.keySet()) {
				LinkedBlockingQueue<V> fails = failover.get(key);
				retries.clear();
				fails.drainTo(retries, output.batchSize);
				if (!retries.isEmpty()) IO.run(() -> output(key, Streams.of(retries)));
			}
		}
	}
}