package net.butfly.albacore.io.faliover;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import net.butfly.albacore.io.Message;
import net.butfly.albacore.io.ext.OpenableThread;
import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.utils.logger.Logger;

public abstract class Failover extends OpenableThread implements Statistical<Failover> {
	protected static final Logger logger = Logger.getLogger(Failover.class);
	protected final FailoverOutput output;
	protected final Constructor<? extends Message> construct;

	protected Failover(String parentName, FailoverOutput output, Class<? extends Message> messageClass) {
		super(parentName + "Failover");
		this.output = output;
		if (null != messageClass) try {
			construct = messageClass.getConstructor(byte[].class);
		} catch (NoSuchMethodException | SecurityException e) {
			throw new RuntimeException("Could not construct " + messageClass.toString() + " from byte[].");
		}
		else construct = null;
	}

	public abstract boolean isEmpty();

	public abstract long size();

	protected abstract long fail(String key, Collection<? extends Message> values);

	private final AtomicInteger paralling = new AtomicInteger();

	protected final long output(String key, Stream<Message> pkg) {
		paralling.incrementAndGet();
		Consumer<Collection<Message>> failing = fails -> {
			fail(key, fails);
		};
		return output.write(key, pkg, failing, s -> {
			try {
				output.commit(key);
			} catch (Exception e) {
				logger().warn(output.name() + " commit failure [" + e.getMessage() + "]");
			} finally {}
		}, 0);
	}
}
