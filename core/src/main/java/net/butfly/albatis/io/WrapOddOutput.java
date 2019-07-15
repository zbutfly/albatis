package net.butfly.albatis.io;

import net.butfly.albacore.lambda.Runnable;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.logger.Statistic;

public abstract class WrapOddOutput<V, V0> extends WrapperBase<OddOutput<V0>> implements OddOutput<V> {
	private static final long serialVersionUID = 2713449285484389905L;

	protected WrapOddOutput(OddOutput<V0> base, String suffix) {
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

	@Override
	public Statistic statistic() {
		return base.statistic();
	}

	@Override
	public void statistic(Statistic s) {
		base.statistic(s);
	}
}
