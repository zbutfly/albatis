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
import net.butfly.albacore.io.utils.Streams;
import net.butfly.albacore.utils.parallel.Parals;

public class HeapFailover<M extends Message> extends Failover<M> {
	private static final int MAX_FAILOVER = 50000;
	private Map<String, LinkedBlockingQueue<M>> failover = new ConcurrentHashMap<>();

	public HeapFailover(String parentName, FailoverOutput<M> output, Function<byte[], M> constructor) throws IOException {
		super(parentName, output, constructor);
		failover = new ConcurrentHashMap<>();
		logger.info(MessageFormat.format("SolrOutput [{0}] failover [memory mode] init.", parentName));
	}

	@Override
	public long size() {
		long c = 0;
		for (String key : failover.keySet())
			c += failover.get(key).size();
		return c;
	}

	@Override
	public boolean isEmpty() {
		return failover.isEmpty();
	}

	@Override
	protected long fail(String key, Collection<? extends M> values) {
		if (opened()) try {
			failover.computeIfAbsent(key, k -> new LinkedBlockingQueue<>(MAX_FAILOVER)).addAll(values);
			return values.size();
		} catch (IllegalStateException ee) {
			logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}]", values.size(), key));
			return 0;
		}
		else {
			logger().error("Failover closed, data lost: " + values.size() + ".");
			return 0;
		}
	}

	@Override
	protected void exec() {
		List<M> retries = new ArrayList<>(output.batchSize);
		while (opened()) {
			for (String key : failover.keySet()) {
				LinkedBlockingQueue<M> fails = failover.get(key);
				retries.clear();
				fails.drainTo(retries, output.batchSize);
				if (!retries.isEmpty()) Parals.run(() -> output(key, Streams.of(retries)));
			}
		}
		if (!failover.isEmpty()) logger().error("Failover closed, data lost: " + failover.size() + ".");
	}
}