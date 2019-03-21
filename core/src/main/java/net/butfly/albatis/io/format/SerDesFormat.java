package net.butfly.albatis.io.format;

import static net.butfly.albacore.utils.collection.Colls.empty;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.alserdes.SerDes;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class SerDesFormat extends RmapFormat {
	private static final long serialVersionUID = -5531474612265588196L;
	private final SerDes sd;
	private final boolean map;

	final boolean addKey = Boolean.parseBoolean(Configs.gets("dataggr.migrate.serdes.addKey", "true"));

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
			Rmap r = m.skeleton();
			Object k = r.key();
			if (null == k || !(k instanceof CharSequence)) {
				if (!addKey) return null;
				k = UUID.randomUUID();
			}
			r.put(k.toString(), v);
			return r;
		} else for (String k : m.keySet())
			if (null != (v = sd.ser(m.get(k)))) m.put(k, v);
		return m;
	}

	@Override
	public Rmap sers(List<Rmap> l) {
		if (empty(l)) return null;
		Qualifier table = l.get(0).table();
		if (map) {
			Object rv = ((MapSerDes) sd).sers(l);
			return null == rv ? null : new Rmap(table, null, Maps.of(UUID.randomUUID().toString(), rv));
		}
		throw new UnsupportedOperationException("Non-MapSerDes does not support list format.");
	}

	@Override
	public Rmap deser(Rmap m) {
		if (empty(m)) return null;
		Object v;
		if (map) {
			if (m.size() > 1) logger().warn("Deser from record contains multiple values with MapSerDes [" + sd.getClass().getSimpleName()
					+ "] only convert first value.");
			for (Entry<String, Object> e : m.entrySet()) {
				Map<String, Object> mm = ((MapSerDes) sd).deser(e.getValue());
				// if (empty(mm)) continue; //XXX: changing kafka now contains an empty value map.
				mm.put(Rmap.RAW_KEY_FIELD, e.getKey());
				return empty(mm) ? null : new Rmap(m.table(), m.key(), mm).keyField(m.keyField()).op(m.op());
			}
			return null;
		} else {
			for (String k : m.keySet())
				if (null != (v = sd.deser(m.get(k)))) m.put(k, v);
			return m;
		}
	}

	@Override
	public List<Rmap> desers(Rmap m) {
		if (empty(m)) return Colls.list();
		if (!map) throw new UnsupportedOperationException("Non-MapSerDes does not support list format.");
		List<Rmap> list = Colls.list();
		m.forEach((k, v) -> ((MapSerDes) sd).desers(v).forEach(e -> list.add(//
				new Rmap(m.table(), m.key(), (Map<String, Object>) e).op(m.op()))//
		));
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
	public Class<?> formatClass() {
		return sd.formatClass();
	}

	@Override
	public String toString() {
		return sd.toString();
	}
}
