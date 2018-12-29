package net.butfly.albatis.io.ext;

import static net.butfly.albacore.paral.Task.waitSleep;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albatis.io.OddQueue;

public class JavaQueue<V> extends OddQueue<V> {
	private static final long serialVersionUID = 4202834153701373076L;
	private BlockingQueue<V> impl;

	@Deprecated
	public JavaQueue(long capacity) {
		super("", capacity);
		this.impl = new LinkedBlockingQueue<>((int) capacity);
		open();
	}

	@Deprecated
	public JavaQueue(BlockingQueue<V> impl, long capacity) {
		super("", capacity);
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
	public boolean enqueue(V e) {
		if (null == e) return false;
		do {} while (opened() && !impl.offer(e) && waitSleep());
		return false;
	}

	@Override
	public V dequeue() {
		if (!opened()) return null;
		V v = null;
		do {} while (opened() && (v = impl.poll()) == null && waitSleep());
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
