package net.butfly.albacore.io;

import net.butfly.albacore.io.queue.QImpl;

public class MapdbQueue<I, O> extends QImpl<I, O> {
	private static final long serialVersionUID = 7491057807684102428L;

	protected MapdbQueue(String name, long capacity) {
		super(name, capacity);
	}

	@Override
	public boolean enqueue0(I d) {
		return false;
	}

	@Override
	public O dequeue0() {
		return null;
	}

	@Override
	public long size() {
		return 0;
	}

	@Override
	public void close() {
		super.close();
	}
}
