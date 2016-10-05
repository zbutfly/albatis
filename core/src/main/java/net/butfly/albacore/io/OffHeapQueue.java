package net.butfly.albacore.io;

import java.io.IOException;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

public class OffHeapQueue extends AbstractQueue<byte[]> implements Queue<byte[]> {
	private static final long serialVersionUID = -1813985267000339980L;
	private static final Logger logger = Logger.getLogger(OffHeapQueue.class);
	protected final String dataFolder;
	protected final IBigQueue queue;

	@Override
	public long size() {
		return queue.size();
	}

	public OffHeapQueue(String name, String dataFolder, long capacity) {
		super("off-heap-queue-" + name, capacity);
		this.dataFolder = dataFolder;
		try {
			logger.info("Off heap queue creating as [" + name + "] at [" + dataFolder + "]");
			queue = new BigQueueImpl(dataFolder, name);
		} catch (IOException e) {
			throw new RuntimeException("Queue create failure", e);
		}
	}

	@Override
	public long enqueue(byte[]... message) {
		while (full())
			if (!Concurrents.waitSleep(FULL_WAIT_MS)) logger.warn("Wait for full interrupted");
		long c = 0;
		for (byte[] m : message)
			try {
				queue.enqueue(m);
				c++;
				stats.in(m.length);
			} catch (IOException e) {
				logger.error("Enqueue failure", e);
			}
		return c;
	}

	@Override
	public void close() {
		super.close();
		try {
			queue.gc();
		} catch (IOException e) {
			logger.error("Queue GC failure", e);
		}
		try {
			queue.close();
		} catch (IOException e) {
			logger.error("Queue close failure", e);
		}
	}

	@Override
	public OffHeapQueue clone() {
		return new OffHeapQueue(dataFolder, name, capacity());
	}

	@Override
	public void gc() {
		super.gc();
		try {
			queue.gc();
		} catch (IOException e) {
			logger.error("Queue GC failure", e);
		}
	}

	@Override
	protected long statsSize(byte[] e) {
		return null == e ? -1 : e.length;
	}

	@Override
	protected boolean enqueueRaw(byte[] e) {
		if (null == e) return false;
		try {
			queue.enqueue(e);
			return true;
		} catch (IOException ex) {
			logger.error("Enqueue failure", ex);
			return false;
		}
	}

	@Override
	protected byte[] dequeueRaw() {
		try {
			return queue.dequeue();
		} catch (IOException e) {
			logger.error("Dequeue failure", e);
			return null;
		}
	}
}
