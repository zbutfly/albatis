package net.butfly.albacore.io.faliover;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;

import net.butfly.albacore.io.OpenableThread;
import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.lambda.Callback;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import scala.Tuple2;

public abstract class Failover<K, V> extends OpenableThread implements Statistical<Failover<K, V>> {
	protected static final Logger logger = Logger.getLogger(Failover.class);
	protected final Writing<K, V> writing;
	protected final int packageSize;
	private final LinkedBlockingQueue<Runnable> tasks;
	private final List<Sender> senders;
	private Callback<K> committing;
	protected final ThreadGroup threadGroup;

	protected Failover(String parentName, Writing<K, V> writing, Callback<K> committing, int packageSize, int parallelism) {
		super(parentName + "Failover");
		threadGroup = new ThreadGroup(name());
		this.writing = writing;
		this.committing = committing;
		this.packageSize = packageSize;
		tasks = new LinkedBlockingQueue<>(parallelism);
		senders = new ArrayList<>();
		for (int i = 0; i < parallelism; i++)
			senders.add(new Sender(parentName, tasks, i));
		trace(packageSize, () -> "failover: " + size());
	}

	public abstract boolean isEmpty();

	public abstract long size();

	protected abstract int fail(K key, Collection<V> values, Exception err);

	@SuppressWarnings("unchecked")
	protected final int doWrite(K key, Collection<V> values, boolean stating) {
		if (values.isEmpty()) return 0;
		int c = 0;
		try {
			c = writing.write(new Tuple2<>(key, values));
			if (stating) stats(values);
		} catch (FailoverException ex) {
			c = fail(key, (Collection<V>) ex.fails.keySet(), ex);
		}
		return c;
	}

	final <W> int enqueueTask(Map<K, ? extends Collection<V>> map) {
		int c = 0;
		for (Entry<K, ? extends Collection<V>> e : map.entrySet()) {
			c += e.getValue().size();
			boolean inserted = false;
			if (opened()) do {
				inserted = tasks.offer(() -> {
					for (Collection<V> pkg : Collections.chopped(e.getValue(), packageSize))
						doWrite(e.getKey(), pkg, false);
					if (committing != null) try {
						committing.call(e.getKey());
					} catch (Exception err) {
						logger.warn("[" + name() + "] commit failure on core [" + e.getKey() + "]", err);
					}
				});
			} while (opened() && !inserted && Concurrents.waitSleep());
			if (!inserted) fail(e.getKey(), e.getValue(), null);
		}
		return c;
	}

	final int tasks() {
		return tasks.size();
	}

	private class Sender extends OpenableThread {
		private LinkedBlockingQueue<Runnable> tasks;

		public Sender(String parentName, LinkedBlockingQueue<Runnable> tasks, int i) {
			super(threadGroup, parentName + "Sender#" + (i + 1));
			this.tasks = tasks;
			start();
		}

		@Override
		public void exec() {
			while (opened()) {
				Runnable r = null;
				try {
					r = tasks.poll(100, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {}
				if (null != r) try {
					r.run();
				} catch (Exception e) {
					logger.error("Sending failure", e);
				}
			}
			logger.debug("Processing remained");
			Runnable remained;
			while ((remained = tasks.poll()) != null)
				try {
					remained.run();
				} catch (Exception e) {
					logger.error("Sending failure", e);
				}
		}
	}

	@Override
	public void close(Runnable run) {
		for (Sender s : senders)
			s.close();
		super.close(run);
	}

	public static class FailoverException extends Exception {
		private static final long serialVersionUID = 4322435055829139356L;
		private final Map<?, String> fails;

		public <V> FailoverException(Map<V, String> fails) {
			super("Output fail:\n\t" + Joiner.on("\n\t").join(fails.values()));
			this.fails = fails;
		}
	}

	@FunctionalInterface
	protected interface Writing<K, V> {
		int write(Tuple2<K, Collection<V>> values) throws FailoverException;
	}
}
