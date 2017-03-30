package net.butfly.albacore.io.faliover;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import net.butfly.albacore.io.Message;
import net.butfly.albacore.io.ext.OpenableThread;
import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.utils.logger.Logger;

public abstract class Failover<K, V extends Message<K, ?, V>> extends OpenableThread implements Statistical<Failover<K, V>> {
	protected static final Logger logger = Logger.getLogger(Failover.class);
	protected final FailoverOutput<K, V> output;
	protected final Function<byte[], ? extends V> construct;

	protected Failover(String parentName, FailoverOutput<K, V> output, Function<byte[], ? extends V> constructor) {
		super(parentName + "Failover");
		this.output = output;
		this.construct = constructor;
	}

	public abstract boolean isEmpty();

	public abstract long size();

	protected abstract long fail(K key, Collection<V> values);

	private final AtomicInteger paralling = new AtomicInteger();

	protected final long output(K key, Stream<V> pkg) {
		Set<V> fails = new ConcurrentSkipListSet<>();
		paralling.incrementAndGet();
		long success = 0;
		try {
			success = output.write(key, pkg, fails);
		} catch (Exception ex) {
			logger().warn(output.name() + " failover write failure", ex);
			fail(key, fails);
		} finally {
			if (logger().isTraceEnabled()) logger.debug("Pending parallelism of [" + output.getClass().getSimpleName() + "]: " + paralling
					.decrementAndGet());
			try {
				output.commit(key);
			} catch (Exception e) {
				logger().warn(output.name() + " commit failure", e);
			}
		}
		if (!fails.isEmpty()) fail(key, fails);
		return success;
	}
}
