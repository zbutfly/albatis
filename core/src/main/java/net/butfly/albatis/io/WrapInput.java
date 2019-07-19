package net.butfly.albatis.io;

import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.lambda.Runnable;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Logger;

public abstract class WrapInput<V, V1> extends WrapperBase<Input<V1>> implements Input<V> {
	private static final long serialVersionUID = 4826171507809605571L;

	protected WrapInput(Input<V1> base, String suffix) {
		super(base.name() + suffix, base);
	}

	@Override
	public abstract void dequeue(Consumer<Sdream<V>> using);

	@Override
	public long capacity() {
		return base.capacity();
	}

	@Override
	public boolean empty() {
		return base.empty();
	}

	@Override
	public boolean full() {
		return base.full();
	}

	@Override
	public Logger logger() {
		return base.logger();
	}

	@Override
	public long size() {
		return base.size();
	}

	@Override
	public boolean opened() {
		return base.opened();
	}

	@Override
	public boolean closed() {
		return base.closed();
	}

	@Override
	public void opening(Runnable handler) {
		base.opening(handler);
	}

	@Override
	public void closing(Runnable handler) {
		base.closing(handler);
	}

	@Override
	public void open() {
		base.open();
	}

	@Override
	public void close() {
		base.close();
	}
}
