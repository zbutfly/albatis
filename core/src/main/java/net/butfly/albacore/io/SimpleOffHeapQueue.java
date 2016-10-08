package net.butfly.albacore.io;

import java.io.IOException;

import net.butfly.albacore.utils.logger.Logger;

public final class SimpleOffHeapQueue extends OffHeapQueueImpl<byte[], byte[]> implements SimpleQueue<byte[]> {
	private static final long serialVersionUID = -1813985267000339980L;
	private static final Logger logger = Logger.getLogger(SimpleOffHeapQueue.class);

	public SimpleOffHeapQueue(String name, String dataFolder, long capacity) {
		super("off-heap-queue-" + name, dataFolder, capacity);
	}

	@Override
	protected final byte[] conv(byte[] e) {
		return e;
	}

	@Override
	protected final byte[] unconv(byte[] e) {
		return e;
	}

	@Override
	protected boolean enqueueRaw(byte[] e) {
		if (null == e) return false;
		try {
			queue.enqueue(e);
			return null != statsRecord(Act.INPUT, e, () -> size());
		} catch (IOException ex) {
			logger.error("Enqueue failure", ex);
			return false;
		}
	}

	@Override
	protected byte[] dequeueRaw() {
		try {
			return statsRecord(Act.OUTPUT, queue.dequeue(), () -> size());
		} catch (IOException e) {
			logger.error("Dequeue failure", e);
			return null;
		}
	}
}
