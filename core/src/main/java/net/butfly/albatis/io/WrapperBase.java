package net.butfly.albatis.io;

import java.io.Serializable;

import net.butfly.albacore.base.Namedly;

public abstract class WrapperBase<B extends IO> extends Namedly implements Wrapper<B>, Serializable {
	private static final long serialVersionUID = 8938418659775645071L;
	protected final B base;

	public WrapperBase() {
		super();
		base = null;
	}

	public WrapperBase(String name, B base) {
		super(name);
		this.base = base;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public final <BB extends IO> BB bases() {
		IO o = base;
		while (o instanceof Wrapper)
			o = ((Wrapper) o).bases();
		return (BB) o;
	}

	@Override
	public int features() {
		return bases().features() | IO.Feature.WRAPPED;
	}
}
