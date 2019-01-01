package net.butfly.albatis.io.format;

import java.util.List;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.alserder.SerDes;
import net.butfly.alserder.format.Format;

public abstract class RmapFormat extends Format<Rmap, TableDesc> implements RmapFormatApi {
	private static final long serialVersionUID = -1644417093671209720L;

	public Rmap assemble(Rmap m, TableDesc... dst) {
		if (null == m || m.isEmpty()) return null;
		return dst.length == 0 ? assemble(m) : assemble(m, match(m.table(), dst).fields());
	}

	public Rmap disassemble(Rmap r, TableDesc... src) {
		if (null == r || r.isEmpty()) return null;
		return src.length == 0 ? disassemble(r) : disassemble(r, match(r.table(), src).fields());
	}

	public Rmap assembles(List<Rmap> l, TableDesc... dst) {
		if (null == l || l.isEmpty()) return null;
		if (dst.length == 0) return assembles(l);
		// TODO
		// else return assembles(l, match(m.table(), dst).fields());
		throw new UnsupportedOperationException("Schemaness record list format not implemented now.");
	}

	public List<Rmap> disassembles(Rmap m, TableDesc... src) {
		if (null == m || m.isEmpty()) return Colls.list();
		if (src.length == 0) return disassembles(m);
		// TODO
		// else return disassembles(l, match(m.table(), dst).fields());
		throw new UnsupportedOperationException("Schemaness record list format not implemented now.");

	}

	public static SerDes.As[] as(Class<?> cls) {
		Class<?> c = cls;
		SerDes.As[] ann;
		while ((ann = c.getAnnotationsByType(SerDes.As.class)).length == 0 && (null != (c = c.getSuperclass())));
		return ann;
	}

	@Override
	public Class<?> rawClass() {
		return Rmap.class;
	}

	private static TableDesc match(String excepted, TableDesc... tables) {
		if (tables.length != 1 && null != excepted) for (TableDesc t : tables)
			if (excepted.equals(t.name)) return t;
		return tables[0];
	}
}
