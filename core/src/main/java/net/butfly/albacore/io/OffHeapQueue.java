package net.butfly.albacore.io;

import java.io.IOException;
import java.util.List;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import net.butfly.albacore.io.queue.QueueImpl;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

public class OffHeapQueue<I, O> extends QueueImpl<I, O> {
	protected static final Logger logger = Logger.getLogger(OffHeapQueue.class);

	protected final String dataFolder;
	protected final IBigQueue queue;
	protected final Converter<byte[], O> oconv;
	protected final Converter<I, byte[]> iconv;

	public OffHeapQueue(String name, String dataFolder, long capacity, Converter<I, byte[]> iconv, Converter<byte[], O> oconv) {
		super(name, capacity);
		this.iconv = iconv;
		this.oconv = oconv;
		this.dataFolder = dataFolder;
		try {
			logger.info("Off heap queue (\"BigQueue\") creating as [" + name + "] at [" + dataFolder + "]");
			queue = new BigQueueImpl(dataFolder, name);
		} catch (IOException e) {
			throw new RuntimeException("Queue create failure", e);
		}
	}

	@Override
	public final long size() {
		return queue.size();
	}

	@Override
	public boolean enqueue(I e, boolean block) {
		if (null == e) return false;
		try {
			byte[] v = iconv.apply(e);
			if (null == v) return true;
			queue.enqueue(v);
			return true;
		} catch (IOException ex) {
			logger.error("Enqueue failure", ex);
			return false;
		}
	}

	@Override
	public O dequeue(boolean block) {
		try {
			return oconv.apply(queue.dequeue());
		} catch (IOException e) {
			logger.error("Dequeue failure", e);
			return null;
		} finally {
			if (queue.isEmpty()) gc();
		}
	}

	@Override
	public final long enqueue(List<I> messages) {
		while (full())
			if (!Concurrents.waitSleep()) logger.warn("Wait for full interrupted");
		long c = 0;
		for (I m : messages)
			if (enqueue(m, false)) c++;
		return c;
	}

	@Override
	public void close() {
		super.close(this::closeLocal);
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
		try {
			queue.gc();
		} catch (IOException e) {
			logger.error("Queue GC failure", e);
		}
	}
}
