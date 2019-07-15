package net.butfly.albatis.io;

import net.butfly.albacore.lambda.Runnable;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.logger.Statistic;

public abstract class WrapOutput<V, V0> extends WrapperBase<Output<V0>> implements Output<V> {
	private static final long serialVersionUID = 7409156522259075670L;

	protected WrapOutput(Output<V0> base, String suffix) {
		super(base.name() + suffix, base);
	}

	@Override
	public abstract void enqueue(Sdream<V> items);

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

	@Override
	public Statistic statistic() {
		return base.statistic();
	}

	@Override
	public void statistic(Statistic s) {
		base.statistic(s);
	}
}
