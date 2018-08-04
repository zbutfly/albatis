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
	public final B bases() {
		B o = base;
		while (o instanceof Wrapper)
			return (B) ((Wrapper) o).bases();
		return o;
	}
}
