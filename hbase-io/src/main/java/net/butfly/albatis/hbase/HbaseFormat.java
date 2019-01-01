package net.butfly.albatis.hbase;

import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.Format;
import net.butfly.alserder.SerDes;

@SerDes.As("hbase")
public class HbaseFormat extends Format {
	private static final long serialVersionUID = 4733354000209088889L;

	@Override
	public Rmap assemble(Rmap v, TableDesc... dst) {
		// TODO Auto-generated method stub
		return super.assemble(v, dst);
	}

	@Override
	public Rmap disassemble(Rmap m, TableDesc... src) {
		// TODO Auto-generated method stub
		return super.disassemble(m, src);
	}
}
