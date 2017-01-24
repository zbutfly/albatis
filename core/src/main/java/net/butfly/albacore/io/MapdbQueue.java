package net.butfly.albacore.io;

import net.butfly.albacore.io.queue.QueueImpl;

public class MapdbQueue<I, O> extends QueueImpl<I, O> {
	protected MapdbQueue(String name, long capacity) {
		super(name, capacity);
	}

	@Override
	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public O dequeue(boolean block) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean enqueue(I item, boolean block) {
		// TODO Auto-generated method stub
		return false;
	}
}
