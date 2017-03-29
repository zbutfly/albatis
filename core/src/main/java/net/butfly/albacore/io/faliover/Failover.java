package net.butfly.albacore.io.faliover;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.base.Joiner;

import net.butfly.albacore.io.IO;
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

	protected abstract long fail(K key, Collection<V> values, Exception err);

	private final AtomicInteger paralling = new AtomicInteger();

	@SuppressWarnings("unchecked")
	protected final long output(K key, Stream<V> pkg) {
		paralling.incrementAndGet();
		List<V> pkgs = IO.list(pkg);
		try {
			return output.write(key, pkgs);
		} catch (FailoverException ex) {
			return fail(key, (Collection<V>) ex.fails.keySet(), ex);
		} catch (Exception ex) {
			return fail(key, pkgs, ex);
		} finally {
			if (logger().isTraceEnabled()) logger.debug("Pending parallelism of [" + output.getClass().getSimpleName() + "]: " + paralling
					.decrementAndGet());
			try {
				output.commit(key);
			} catch (Exception e) {
				logger().warn(output.name() + " commit failure", e);
			}
		}
	}

	public static class FailoverException extends RuntimeException {
		private static final long serialVersionUID = 4322435055829139356L;
		protected final Map<?, String> fails;

		public <V> FailoverException(Map<V, String> fails) {
			super("Output fail:\n\t" + Joiner.on("\n\t").join(fails.values()));
			this.fails = fails;
		}
	}

	@FunctionalInterface
	protected interface Writing<K, V> {
		int write(K partition, Collection<V> pkg) throws FailoverException;
	}

	// @Override
	// public boolean opened() {
	// return super.opened() && output.opened();
	// }
}
