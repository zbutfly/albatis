package net.butfly.albacore.io.faliover;

import net.butfly.albacore.io.OpenableThread;
import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.lambda.ConverterPair;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.logger.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public abstract class Failover<K, V> extends OpenableThread implements Statistical<Failover<K, V>> {
	private static final long serialVersionUID = -7515454826294115208L;
	protected static final Logger logger = Logger.getLogger(Failover.class);
	protected final ConverterPair<K, List<V>, Exception> writing;
	protected final int packageSize;
	private final LinkedBlockingQueue<Runnable> tasks;
	private final List<Sender> senders;
	private Consumer<K> committing;

	protected Failover(String parentName, ConverterPair<K, List<V>, Exception> writing, Consumer<K> committing, int packageSize,
			int parallelism) {
		super(parentName + "Failover");
		this.writing = writing;
		this.committing = committing;
		this.packageSize = packageSize;
		tasks = new LinkedBlockingQueue<>(parallelism);
		senders = new ArrayList<>();
		for (int i = 0; i < parallelism; i++)
			senders.add(new Sender(parentName, tasks, i));
		trace(packageSize, () -> "failover: " + size());
	}

	@Override
	protected void exec() {
		while (opened())
			retry();
	}

	public abstract boolean isEmpty();

	public abstract long size();

	protected abstract int fail(K core, List<V> docs, Exception err);

	protected abstract void retry();

	public final <W> void dotry(Map<K, List<V>> map) {
		for (Entry<K, List<V>> e : map.entrySet()) {
			boolean inserted = false;
			if (opened()) do {
				inserted = tasks.offer(() -> {
					for (List<V> pkg : Collections.chopped(e.getValue(), packageSize)) {
						Exception err = writing.apply(e.getKey(), pkg);
						if (null != err) fail(e.getKey(), pkg, err);
					}
					if (committing != null) try {
						committing.accept(e.getKey());
					} catch (Exception err) {
						logger.warn("[" + name() + "] commit failure on core [" + e.getKey() + "]", err);
					}
				});
			} while (opened() && !inserted);
			if (!inserted) fail(e.getKey(), e.getValue(), null);
		}
	}

	private class Sender extends OpenableThread {
		private LinkedBlockingQueue<Runnable> tasks;

		public Sender(String parentName, LinkedBlockingQueue<Runnable> tasks, int i) {
			super(parentName + "Sender#" + (i + 1));
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
		super.close();
		for (Sender s : senders)
			s.close();
	}
}
