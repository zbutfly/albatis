package net.butfly.albatis.io.format;

import net.butfly.albatis.io.Rmap;
import net.butfly.alserder.SerDes;

@SerDes.As("")
@SerDes.As("const")
class ConstFormat implements Format {
	private static final long serialVersionUID = 4665610987994353342L;

	@Override
	public Rmap ser(Rmap m) {
		return m;
	}

	@Override
	public Rmap deser(Rmap m) {
		return m;
	}
}
