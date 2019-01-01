package net.butfly.albatis.io.format;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.alserder.SerDes;

public abstract class Format implements Loggable, Serializable {
	private static final long serialVersionUID = -1644417093671209720L;
	public static Format CONST_FORMAT = new ConstFormat();
	private SerDes.As as;

	protected Format() {}

	// loop invoking, need override at least one pair of methods.

	public Rmap assemble(Rmap m, TableDesc... dst) {
		if (null == m || m.isEmpty()) return null;
		Rmap r = assembles(Colls.list(m), dst);
		return null == r || r.isEmpty() ? null : r;
	}

	public Rmap disassemble(Rmap m, TableDesc... src) {
		if (null == m || m.isEmpty()) return null;
		List<Rmap> l = disassembles(m, src);
		return null == l || l.isEmpty() ? null : l.get(0);
	}

	public Rmap assembles(List<Rmap> l, TableDesc... dst) {
		if (null == l || l.isEmpty()) return null;
		if (l.size() > 1) //
			logger().warn(getClass() + " does not support multiple serializing, only first will be writen: \n\t" + l.toString());
		Rmap first = l.get(0);
		return null == first ? null : assemble(first, dst);
	}

	public List<Rmap> disassembles(Rmap m, TableDesc... src) {
		if (null == m || m.isEmpty()) return Colls.list();
		Rmap r = disassemble(m, src);
		return null == r || r.isEmpty() ? Colls.list() : Colls.list(r);
	}

	private static final Map<String, Format> FORMATS = Maps.of();
	private static Logger logger = Logger.getLogger(Format.class);

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <F extends Format> F of(String format) {
		return (F) FORMATS.computeIfAbsent(format, f -> {
			Set<Class<? extends Format>> fcls = Reflections.getSubClasses(Format.class);
			Format ff;
			for (Class<? extends Format> c : fcls)
				for (SerDes.As as : c.getAnnotationsByType(SerDes.As.class))
					if (format.equals(as.value()) && null != (ff = Reflections.construct(c))) return ff.as(as);

			logger.debug("Format [" + format + "] not found, scanning for SerDes.");
			Set<Class<? extends SerDes>> sdcls = Reflections.getSubClasses(SerDes.class);
			for (Class<? extends SerDes> c : sdcls)
				for (SerDes.As as : c.getAnnotationsByType(SerDes.As.class))
					if (format.equals(as.value())) return new SerDesFormat(Reflections.construct(c)).as(as);

			logger.warn("Format [" + format + "] not found, values will not be changed.");
			return CONST_FORMAT;
		});
	}

	public static SerDes.As[] as(Class<?> cls) {
		Class<?> c = cls;
		SerDes.As[] ann;
		while ((ann = c.getAnnotationsByType(SerDes.As.class)).length == 0 && (null != (c = c.getSuperclass())));
		return ann;
	}

	public SerDes.As as() {
		return as;
	}

	public Format as(SerDes.As as) {
		this.as = as;
		return this;
	}

	public Class<?> rawClass() {
		return Rmap.class;
	}
}
