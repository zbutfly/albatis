package net.butfly.albacore.io;

import java.io.IOException;

import com.bluejeans.bigqueue.BigQueue;

import net.butfly.albacore.io.queue.QueueImpl;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.logger.Logger;

public class OffHeapQueue<I, O> extends QueueImpl<I, O> {
	protected static final Logger logger = Logger.getLogger(OffHeapQueue.class);

	protected final String dataFolder;
	protected final BigQueue queue;
	protected final Converter<byte[], O> oconv;
	protected final Converter<I, byte[]> iconv;

	public OffHeapQueue(String name, String dataFolder, long capacity, Converter<I, byte[]> iconv, Converter<byte[], O> oconv) {
		super(name, capacity);
		this.iconv = iconv;
		this.oconv = oconv;
		this.dataFolder = dataFolder;
		logger.info("Off heap queue (\"BigQueue\") creating as [" + name + "] at [" + dataFolder + "]");
		queue = new BigQueue(dataFolder, name);
	}

	@Override
	public final long size() {
		return queue.size();
	}

	@Override
	protected boolean enqueue(I e) {
		if (null == e) return false;
		byte[] v = iconv.apply(e);
		if (null == v) return true;
		queue.enqueue(v);
		return true;
	}

	@Override
	protected O dequeue() {
		try {
			return oconv.apply(queue.dequeue());
		} finally {
			if (queue.isEmpty()) gc();
		}
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
		queue.gc();
	}
}
