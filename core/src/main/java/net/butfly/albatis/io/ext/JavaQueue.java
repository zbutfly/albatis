package net.butfly.albatis.io.ext;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albacore.utils.parallel.Concurrents;
import net.butfly.albatis.io.QueueOddImpl;

public class JavaQueue<V> extends QueueOddImpl<V> {
	private BlockingQueue<V> impl;

	public JavaQueue(long capacity) {
		super(capacity);
		this.impl = new LinkedBlockingQueue<>((int) capacity);
		open();
	}

	public JavaQueue(BlockingQueue<V> impl, long capacity) {
		super(capacity);
		this.impl = impl;
		open();
	}

	public JavaQueue(String name, long capacity) {
		super(name, capacity);
		this.impl = new LinkedBlockingQueue<>((int) capacity);
		open();
	}

	public JavaQueue(String name, BlockingQueue<V> impl, long capacity) {
		super(name, capacity);
		this.impl = impl;
		open();
	}

	@Override
	protected final boolean enqueue(V e) {
		if (null == e) return false;
		do {} while (opened() && !impl.offer(e) && Concurrents.waitSleep());
		return false;
	}

	@Override
	protected V dequeue() {
		if (!opened()) return null;
		V v = null;
		do {} while (opened() && (v = impl.poll()) == null && Concurrents.waitSleep());
		return v;
	}

	@Override
	public boolean empty() {
		return impl.isEmpty();
	}

	@Override
	public boolean full() {
		return impl.remainingCapacity() <= 0;
	}

	@Override
	public long size() {
		return impl.size();
	}
}
