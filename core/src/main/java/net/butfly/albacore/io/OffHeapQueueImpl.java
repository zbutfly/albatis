package net.butfly.albacore.io;

import java.io.IOException;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

public abstract class OffHeapQueueImpl<I, O> extends QueueImpl<I, O, byte[]> implements Queue<I, O> {
	private static final long serialVersionUID = -1L;
	private static final Logger logger = Logger.getLogger(OffHeapQueueImpl.class);

	protected final String dataFolder;
	protected final IBigQueue queue;

	public OffHeapQueueImpl(String name, String dataFolder, long capacity) {
		super(name, capacity);
		this.dataFolder = dataFolder;
		try {
			logger.info("Off heap queue (\"BigQueue\") creating as [" + name + "] at [" + dataFolder + "]");
			queue = new BigQueueImpl(dataFolder, name);
		} catch (IOException e) {
			throw new RuntimeException("Queue create failure", e);
		}
	}

	protected abstract byte[] conv(I e);

	protected abstract O unconv(byte[] e);

	@Override
	public final long size() {
		return queue.size();
	}

	@Override
	protected boolean enqueueRaw(I e) {
		if (null == e) return false;
		try {
			queue.enqueue(stats(Act.INPUT, conv(e)));
			return true;
		} catch (IOException ex) {
			logger.error("Enqueue failure", ex);
			return false;
		}
	}

	@Override
	protected O dequeueRaw() {
		try {
			return unconv(stats(Act.OUTPUT, queue.dequeue()));
		} catch (IOException e) {
			logger.error("Dequeue failure", e);
			return null;
		}
	}

	@SafeVarargs
	@Override
	public final long enqueue(I... message) {
		while (full())
			if (!Concurrents.waitSleep(FULL_WAIT_MS)) logger.warn("Wait for full interrupted");
		long c = 0;
		for (I m : message)
			if (enqueueRaw(m)) c++;
		return c;
	}

	@Override
	public final void close() {
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
	public final void gc() {
		super.gc();
		try {
			queue.gc();
		} catch (IOException e) {
			logger.error("Queue GC failure", e);
		}
	}
}
