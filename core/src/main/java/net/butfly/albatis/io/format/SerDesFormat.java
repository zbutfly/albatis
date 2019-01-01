package net.butfly.albatis.io.format;

import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.alserder.SerDes;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class SerDesFormat extends Format {
	private static final long serialVersionUID = -5531474612265588196L;
	private final SerDes sd;

	public SerDesFormat(SerDes sd) {
		super();
		this.sd = sd;
	}

	@Override
	public Rmap assemble(Rmap m, TableDesc... dst) {
		if (null == m || m.isEmpty()) return null;
		for (String k : m.keySet())
			m.put(k, sd.ser(m.get(k)));
		return m;
	}

	@Override
	public Rmap disassemble(Rmap m, TableDesc... src) {
		if (null == m || m.isEmpty()) return null;
		for (String k : m.keySet())
			m.put(k, sd.deser(m.get(k)));
		return m;
	}
}
