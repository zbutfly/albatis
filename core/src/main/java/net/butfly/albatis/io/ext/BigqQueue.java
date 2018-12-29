package net.butfly.albatis.io.ext;

import static net.butfly.albacore.paral.Task.waitSleep;

import java.io.IOException;

import com.bluejeans.bigqueue.BigQueue;

import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.OddQueue;

public class BigqQueue<V> extends OddQueue<V> {
	private static final long serialVersionUID = -6105461013291834558L;
	public static final long GC_INTV = Integer.parseInt(System.getProperty("albatis.queue.bigqueue.autogc", "30")) * 1000;
	protected static final Logger logger = Logger.getLogger(BigqQueue.class);

	protected final String dataFolder;
	protected final BigQueue queue;
	protected final Constructor<V> oconv;
	protected final Destructor<V> iconv;

	public BigqQueue(String dataFolder, long capacity, Destructor<V> iconv, Constructor<V> oconv) {
		this("BigQueue", dataFolder, capacity, iconv, oconv);
	}

	public BigqQueue(String name, String dataFolder, long capacity, Destructor<V> iconv, Constructor<V> oconv) {
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
			} while (opened() && waitSleep(GC_INTV));
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
	public boolean enqueue(V e) {
		if (null == e) return false;
		byte[] v;
		try {
			v = iconv.destruct(e);
		} catch (IOException e1) {
			return false;
		}
		if (null == v) return false;
		while (full())
			waitSleep();
		queue.enqueue(v);
		return true;
	}

	@Override
	public V dequeue() {
		V v = null;
		try {
			while (v == null && !empty() && opened()) {
				byte[] buf = queue.dequeue();
				if (null == buf) continue;
				try {
					v = oconv.construct(buf);
				} catch (IOException e) {}
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

	public interface Constructor<M> {
		M construct(byte[] data) throws IOException;
	}

	public interface Destructor<M> {
		byte[] destruct(M msg) throws IOException;
	}
}