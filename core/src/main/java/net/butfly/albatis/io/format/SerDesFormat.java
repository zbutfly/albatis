package net.butfly.albatis.io.format;

import static net.butfly.albacore.utils.collection.Colls.empty;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.alserder.SerDes;
import net.butfly.alserder.format.Format;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class SerDesFormat extends Format<Rmap, TableDesc> {
	private static final long serialVersionUID = -5531474612265588196L;
	private final SerDes sd;
	private final boolean map;

	public SerDesFormat(SerDes sd) {
		super();
		this.sd = sd;
		map = sd instanceof MapSerDes;
	}

	@Override
	public Rmap ser(Rmap m) {
		if (empty(m)) return null;
		Object v;
		if (map) {
			if (null == (v = ((MapSerDes) sd).ser(m))) return null;
			m.clear();
			m.put(UUID.randomUUID().toString(), v);
			return m;
		}
		for (String k : m.keySet()) if (null != (v = sd.ser(m.get(k)))) m.put(k, v);
		return m;
	}

	@Override
	public Rmap deser(Rmap m) {
		if (empty(m)) return null;
		Object v;
		for (String k : m.keySet()) if (null != (v = sd.deser(m.get(k)))) m.put(k, v);
		return m;
	}

	@Override
	public Rmap sers(List<Rmap> l) {
		if (empty(l)) return null;
		if (map) return (Rmap) ((MapSerDes) sd).sers(l);
		throw new UnsupportedOperationException("Non-MapSerDes does not support list format.");
	}

	public List<Rmap> desers(Rmap m) {
		if (empty(m)) return null;
		if (!map) throw new UnsupportedOperationException("Non-MapSerDes does not support list format.");
		List<Rmap> list = Colls.list();
		m.forEach((k, v) -> ((MapSerDes) sd).desers(v)
				.forEach(e -> list.add(new Rmap(m.table(), m.key(), (Map<String, Object>) e).op(m.op()))));
		return list;
	}

	// sd serdes does not support schema process
	@Override
	public Rmap ser(Rmap m, TableDesc extra) {
		return ser(m);
	}

	@Override
	public Rmap deser(Rmap m, TableDesc extra) {
		return deser(m);
	}

	@Override
	public Rmap sers(List<Rmap> l, TableDesc extra) {
		return sers(l);
	}

	@Override
	public List<Rmap> desers(Rmap m, TableDesc extra) {
		return desers(m);
	}

	@Override
	public Class<?> rawClass() {
		return sd.rawClass();
	}

	@Override
	public String toString() {
		return sd.toString();
	}
}
