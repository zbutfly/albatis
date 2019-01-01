package net.butfly.albatis.io.format;

import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.alserder.SerDes;

@SerDes.As("")
@SerDes.As("const")
class ConstFormat extends Format {
	private static final long serialVersionUID = 4665610987994353342L;

	@Override
	public Rmap assemble(Rmap m, TableDesc... dst) {
		return m;
	}

	@Override
	public Rmap disassemble(Rmap m, TableDesc... src) {
		return m;
	}
}
