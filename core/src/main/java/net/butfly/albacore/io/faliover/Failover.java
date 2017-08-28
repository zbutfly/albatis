package net.butfly.albacore.io.faliover;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import net.butfly.albacore.io.Message;
import net.butfly.albacore.io.ext.OpenableThread;
import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.utils.logger.Logger;

public abstract class Failover<M extends Message> extends OpenableThread implements Statistical<Failover<M>> {
	protected static final Logger logger = Logger.getLogger(Failover.class);
	protected final FailoverOutput<M> output;
	protected final Function<byte[], M> construct;

	protected Failover(String parentName, FailoverOutput<M> output, Function<byte[], M> constructor) {
		super(parentName + "Failover");
		this.output = output;
		this.construct = constructor;
	}

	public abstract boolean isEmpty();

	public abstract long size();

	protected abstract long fail(String key, Collection<? extends M> values);

	private final AtomicInteger paralling = new AtomicInteger();

	protected final long output(String key, Stream<? extends M> pkg) {
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
