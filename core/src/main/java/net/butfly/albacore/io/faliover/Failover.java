package net.butfly.albacore.io.faliover;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import net.butfly.albacore.io.Record;
import net.butfly.albacore.io.ext.OpenableThread;
import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.utils.logger.Logger;

public abstract class Failover extends OpenableThread implements Statistical<Failover> {
	protected static final Logger logger = Logger.getLogger(Failover.class);
	protected final FailoverOutput output;
	protected final Function<byte[], Record> construct;

	protected Failover(String parentName, FailoverOutput output, Function<byte[], Record> constructor) {
		super(parentName + "Failover");
		this.output = output;
		this.construct = constructor;
	}

	public abstract boolean isEmpty();

	public abstract long size();

	protected abstract long fail(String key, Collection<? extends Record> values);

	private final AtomicInteger paralling = new AtomicInteger();

	protected final long output(String key, Stream<? extends Record> pkg) {
		paralling.incrementAndGet();
		return output.write(key, pkg, fails -> {
			fail(key, fails);
		}, s -> {
			try {
				output.commit(key);
			} catch (Exception e) {
				logger().warn(output.name() + " commit failure [" + e.getMessage() + "]");
			} finally {}
		}, 0);
	}
}
