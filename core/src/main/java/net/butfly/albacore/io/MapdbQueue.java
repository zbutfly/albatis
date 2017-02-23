package net.butfly.albacore.io;

import net.butfly.albacore.io.queue.QueueImpl;

public class MapdbQueue<I, O> extends QueueImpl<I, O> {
	protected MapdbQueue(String name, long capacity) {
		super(name, capacity);
	}

	@Override
	public long size() {
		return 0;
	}

	@Override
	protected O dequeue() {
		return null;
	}

	@Override
	protected boolean enqueue(I item) { 
		return false;
	}
}
