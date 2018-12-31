package net.butfly.albatis.hbase;

import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.Format;
import net.butfly.alserder.SerDes;

@SerDes.As("hbase")
public class HbaseFormat implements Format {
	@Override
	public Rmap ser(Rmap v) {
		// TODO Auto-generated method stub
		return Format.super.ser(v);
	}

	@Override
	public Rmap deser(Rmap m) {
		// TODO Auto-generated method stub
		return Format.super.deser(m);
	}

	private static final long serialVersionUID = 4733354000209088889L;

}
