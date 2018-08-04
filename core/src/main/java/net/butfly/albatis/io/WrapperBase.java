package net.butfly.albatis.io;

import net.butfly.albacore.base.Namedly;

public abstract class WrapperBase<B extends IO> extends Namedly implements Wrapper<B> {
	protected final B base;

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
