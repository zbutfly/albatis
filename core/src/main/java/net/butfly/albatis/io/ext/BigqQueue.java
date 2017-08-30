package net.butfly.albatis.io.ext;

import java.io.IOException;
import java.util.function.Function;

import com.bluejeans.bigqueue.BigQueue;

import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.parallel.Concurrents;
import net.butfly.albatis.io.QueueOddImpl;

public class BigqQueue<V> extends QueueOddImpl<V> {
	public static final long GC_INTV = Integer.parseInt(System.getProperty("albatis.queue.bigqueue.autogc", "30")) * 1000;
	protected static final Logger logger = Logger.getLogger(BigqQueue.class);

	protected final String dataFolder;
	protected final BigQueue queue;
	protected final Function<byte[], V> oconv;
	protected final Function<V, byte[]> iconv;

	public BigqQueue(String name, String dataFolder, long capacity, Function<V, byte[]> iconv, Function<byte[], V> oconv) {
		super(name, capacity);
		this.iconv = iconv;
		this.oconv = oconv;
		this.dataFolder = dataFolder;
		logger.info("Off heap queue (\"BigQueue\") creating as [" + name + "] at [" + dataFolder + "]");
		queue = new BigQueue(dataFolder, name);
		closing(this::closeLocal);
		Thread gc = new Thread(() -> {
			do {
				try {
					queue.gc();
				} catch (Throwable t) {
					logger().error("BigQueue gc fail", t);
				}
			} while (opened() && Concurrents.waitSleep(GC_INTV));
		}, "BigQueue-Maintainancer-Daemon-Thread");
		gc.setDaemon(true);
		gc.start();
		open();
	}

	@Override
	public final long size() {
		return queue.size();
	}

	@Override
	protected boolean enqueue(V e) {
		if (null == e) return false;
		byte[] v = iconv.apply(e);
		if (null == v) return false;
		queue.enqueue(v);
		return true;
	}

	@Override
	protected V dequeue() {
		V v = null;
		try {
			while (v == null && !empty() && opened()) {
				byte[] buf = queue.dequeue();
				if (null == buf) continue;
				v = this.oconv.apply(buf);
			}
		} finally {
			if (queue.isEmpty()) gc();
		}
		return v;
	}

	private void closeLocal() {
		gc();
		try {
			queue.close();
		} catch (IOException e) {
			logger.error("Queue close failure", e);
		}
	}

	public final void gc() {
		queue.gc();
	}
}