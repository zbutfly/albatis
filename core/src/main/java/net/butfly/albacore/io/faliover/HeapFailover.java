package net.butfly.albacore.io.faliover;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import net.butfly.albacore.lambda.ConverterPair;

public class HeapFailover<K, V> extends Failover<K, V> {
	private static final long serialVersionUID = -7766759011944551301L;
	private static final int MAX_FAILOVER = 50000;
	private Map<K, LinkedBlockingQueue<V>> failover = new ConcurrentHashMap<>();

	public HeapFailover(String parentName, ConverterPair<K, List<V>, Exception> writing, Consumer<K> committing, int packageSize,
			int parallelism) throws IOException {
		super(parentName, writing, committing, packageSize, parallelism);
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
	public int fail(K core, List<V> docs, Exception err) {
		LinkedBlockingQueue<V> fails = failover.computeIfAbsent(core, k -> new LinkedBlockingQueue<>(MAX_FAILOVER));
		try {
			fails.addAll(docs);
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
	protected void retry() {
		List<V> retries = new ArrayList<>(packageSize);
		for (K core : failover.keySet()) {
			LinkedBlockingQueue<V> fails = failover.get(core);
			fails.drainTo(retries, packageSize);
			stats(retries);
			if (!retries.isEmpty()) {
				Exception e = writing.apply(core, retries);
				if (null != e) fail(core, retries, e);
				retries.clear();
			}
		}
	}

}