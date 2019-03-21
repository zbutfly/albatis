package net.butfly.albatis.io.format;

import static net.butfly.albacore.utils.collection.Colls.empty;

import java.util.List;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.alserdes.SerDes;
import net.butfly.alserdes.format.Format;

public abstract class RmapFormat extends Format<Rmap, TableDesc> {
	private static final long serialVersionUID = -1644417093671209720L;

	@Override
	public Rmap ser(Rmap m, TableDesc dst) {
		if (empty(m)) return null;
		return null == dst ? ser(m) : ser(m, match(m.table(), dst).fields());
	}

	@Override
	public Rmap deser(Rmap r, TableDesc src) {
		if (empty(r)) return null;
		return null == src ? deser(r) : deser(r, match(r.table(), src).fields());
	}

	@Override
	public Rmap sers(List<Rmap> l, TableDesc dst) {
		if (empty(l)) return null;
		if (null == dst) return sers(l);
		// TODO
		// else return sers(l, match(m.table(), dst).fields());
		throw new UnsupportedOperationException("Schemaness record list format not implemented now.");
	}

	@Override
	public List<Rmap> desers(Rmap m, TableDesc src) {
		if (empty(m)) return Colls.list();
		if (null == src) return desers(m);
		// TODO
		// else return desers(l, match(m.table(), dst).fields());
		throw new UnsupportedOperationException("Schemaness record list format not implemented now.");

	}

	public static SerDes.As[] as(Class<?> cls) {
		Class<?> c = cls;
		SerDes.As[] ann;
		while ((ann = c.getAnnotationsByType(SerDes.As.class)).length == 0 && (null != (c = c.getSuperclass())));
		return ann;
	}

	@Override
	public Class<?> formatClass() {
		return Rmap.class;
	}

	private static TableDesc match(Qualifier excepted, TableDesc... tables) {
		if (tables.length != 1 && null != excepted) for (TableDesc t : tables)
			if (excepted.equals(t.qualifier)) return t;
		return tables[0];
	}

	/**
	 * <b>Schemaness</b> record assemble.<br>
	 * default as if schemaless
	 */
	protected Rmap ser(Rmap src, FieldDesc... fields) {
		return ser(src); //
	}

	/**
	 * <b>Schemaness</b> record disassemble.<br>
	 * default as if schemaless
	 */
	protected Rmap deser(Rmap dst, FieldDesc... fields) {
		return deser(dst);
	}

	// TODO: List schemaness dis/assembling

}
