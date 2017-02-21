package net.butfly.albacore.io.faliover;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import com.google.common.base.Joiner;

import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.Message;
import net.butfly.albacore.io.OpenableThread;
import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.logger.Logger;

public abstract class Failover<K, V extends Message<K, ?, V>> extends OpenableThread implements Statistical<Failover<K, V>> {
	protected static final Logger logger = Logger.getLogger(Failover.class);
	protected final FailoverOutput<K, V> output;
	protected final Function<byte[], ? extends V> construct;

	protected Failover(String parentName, FailoverOutput<K, V> output, Function<byte[], ? extends V> constructor) {
		super(parentName + "Failover");
		this.output = output;
		this.construct = constructor;
		// trace(packageSize, () -> "failover: " + size());
	}

	public abstract boolean isEmpty();

	public abstract long size();

	protected abstract void fail(K key, Collection<V> values, Exception err);

	@SuppressWarnings("unchecked")
	protected final long output(K key, Collection<V> pkg) {
		IO.io.each(Collections.chopped(pkg, output.packageSize), p -> {
			long now = System.currentTimeMillis();
			try {
				output.write(key, p);
			} catch (FailoverException ex) {
				fail(key, (Collection<V>) ex.fails.keySet(), ex);
			} finally {
				IO.io.run(() -> logger().debug(() -> "Package [" + p.size() + "/" + pkg.size() + "] output/failover finished in [" + (System
						.currentTimeMillis() - now) + "] ms, sample key: [" + p.get(0).id() + "]."));
			}
		});
		try {
			output.commit(key);
		} catch (Exception e) {
			logger().warn(output.name() + " commit failure", e);
		}
		return pkg.size();
	}

	@Override
	public void open(Runnable opening) {
		super.open(opening);
	}

	@Override
	public void close(Runnable run) {
		super.close(run);
	}

	public static class FailoverException extends Exception {
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
}
