package net.butfly.albatis.io.format;

import java.util.Map;

import net.butfly.albatis.io.Rmap;
import net.butfly.alserder.SerDes;

public class MapSerDesFormat extends RmapFormat {
	private static final long serialVersionUID = -1201642827803301187L;
	private final SerDes<Map<String, Object>, Map<String, Object>> sd;

	public MapSerDesFormat(SerDes<Map<String, Object>, Map<String, Object>> sd) {
		super();
		this.sd = sd;
	}

	@Override
	public Rmap assemble(Rmap m) {
		Object k = m.key();
		Map<String, Object> data = sd.ser(m.map());
		m.clear();
		m.putAll(data);
		if (null != k && null == m.key()) m.key(k);
		return m;
	}

	@Override
	public Rmap disassemble(Rmap r) {
		if (null == r || r.isEmpty()) return null;
		Object k = r.key();
		Map<String, Object> data = sd.ser(r.map());
		r.clear();
		r.putAll(data);
		if (null != k && null == r.key()) r.key(k);
		return r;
	}
}
