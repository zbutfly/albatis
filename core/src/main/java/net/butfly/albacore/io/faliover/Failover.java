package net.butfly.albacore.io.faliover;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import net.butfly.albacore.io.OpenableThread;
import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.lambda.Callback;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.logger.Logger;
import scala.Tuple2;

public abstract class Failover<K, V> extends OpenableThread implements Statistical<Failover<K, V>> {
	private static final long serialVersionUID = -7515454826294115208L;
	protected static final Logger logger = Logger.getLogger(Failover.class);
	protected final Converter<Tuple2<K, List<V>>, Integer> writing;
	protected final int packageSize;
	private final LinkedBlockingQueue<Runnable> tasks;
	private final List<Sender> senders;
	private Callback<K> committing;
	protected final ThreadGroup threadGroup;

	protected Failover(String parentName, Converter<Tuple2<K, List<V>>, Integer> writing, Callback<K> committing, int packageSize,
			int parallelism) {
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

	protected abstract int fail(K key, List<V> values, Exception err);

	protected final int doWrite(K key, List<V> values, boolean stating) {
		if (values.isEmpty()) return 0;
		int c = 0;
		try {
			c = writing.apply(new Tuple2<>(key, values));
			if (stating) stats(values);
		} catch (Exception ex) {
			c = fail(key, values, ex);
		}
		return c;
	}

	final <W> int insertTask(Map<K, List<V>> map) {
		int c = 0;
		for (Entry<K, List<V>> e : map.entrySet()) {
			boolean inserted = false;
			if (opened()) do {
				c += e.getValue().size();
				inserted = tasks.offer(() -> {
					for (List<V> pkg : Collections.chopped(e.getValue(), packageSize))
						doWrite(e.getKey(), pkg, false);
					if (committing != null) try {
						committing.call(e.getKey());
					} catch (Exception err) {
						logger.warn("[" + name() + "] commit failure on core [" + e.getKey() + "]", err);
					}
				});
			} while (opened() && !inserted);
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
	public void close() {
		for (Sender s : senders)
			s.close();
		super.close();
	}
}
