package net.butfly.albatis.io;

import net.butfly.albacore.lambda.Runnable;
import net.butfly.albacore.utils.logger.Logger;

public abstract class WrapOddInput<V, V1> extends WrapperBase<OddInput<V1>> implements OddInput<V> {
	private static final long serialVersionUID = -8501308891027830037L;

	protected WrapOddInput(OddInput<V1> base, String suffix) {
		super(base.name() + suffix, base);
	}

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
