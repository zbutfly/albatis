package net.butfly.albatis.io.format;

import java.util.List;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Rmap;
import net.butfly.alserder.SerDes;

public interface Format extends SerDes<Rmap, Rmap> {
	default Rmap deser(Rmap m) {
		if (null == m || m.isEmpty()) return null;
		List<Rmap> l = desers(m);
		return null == l || l.isEmpty() ? null : l.get(0);
	}

	static Format forMap(SerDes<Map<String, Object>, Map<String, Object>> sd) {
		return new Format() {
			private static final long serialVersionUID = -1201642827803301187L;

			@Override
			public Rmap ser(Rmap m) {
				Object k = m.key();
				Map<String, Object> data = sd.ser(m.map());
				m.clear();
				m.putAll(data);
				if (null != k && null == m.key()) m.key(k);
				return m;
			}

			@Override
			public Rmap deser(Rmap r) {
				if (null == r || r.isEmpty()) return null;
				Object k = r.key();
				Map<String, Object> data = sd.ser(r.map());
				r.clear();
				r.putAll(data);
				if (null != k && null == r.key()) r.key(k);
				return r;
			}
		};
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	static <T> Format forValue(SerDes sd) {
		if (null == sd) return null;
		logger.debug("Format construct by SerDes [" + sd.getClass().getName() + "]");
		return null == sd ? null : new Format() {
			private static final long serialVersionUID = -5531474612265588196L;

			@Override
			public Rmap ser(Rmap m) {
				if (null == m || m.isEmpty()) return null;
				for (String k : m.keySet())
					m.put(k, sd.ser(m.get(k)));
				return m;
			}

			@Override
			public Rmap deser(Rmap m) {
				if (null == m || m.isEmpty()) return null;
				for (String k : m.keySet())
					m.put(k, sd.deser(m.get(k)));
				return m;
			}

			@Override
			public Class<?> rawClass() {
				return sd.rawClass();
			}
		};
	}

	static final Map<String, Format> FORMATS = Maps.of();
	static final Map<String, SerDes.As> FORMATS_AS = Maps.of();
	static Logger logger = Logger.getLogger(Format.class);

	@SuppressWarnings({ "unchecked", "rawtypes" })
	static <F extends Format> F of(String format) {
		return (F) FORMATS.computeIfAbsent(format, f -> {
			Set<Class<? extends Format>> fcls = Reflections.getSubClasses(Format.class);
			Format ff;
			for (Class<? extends Format> c : fcls) {
				for (As as : c.getAnnotationsByType(SerDes.As.class))
					if (format.equals(as.value()) && null != (ff = Reflections.construct(c))) {
						FORMATS_AS.put(f, as);
						return ff;
					}
			}

			Set<Class<? extends SerDes>> sdcls = Reflections.getSubClasses(SerDes.class);
			logger.debug("Format [" + format + "] not found, scanning for SerDes.");
			for (Class<? extends SerDes> c : sdcls) {
				for (As as : c.getAnnotationsByType(SerDes.As.class))
					if (format.equals(as.value()) && null != (ff = forValue(Reflections.construct(c)))) {
						FORMATS_AS.put(f, as);
						return ff;
					}
			}
			logger.warn("Format [" + format + "] not found, values will not be changed.");
			return CONST_FORMAT;
		});
	}

	static SerDes.As asOf(String format) {
		of(format);
		return FORMATS_AS.get(format);
	}

	static SerDes.As[] as(Class<?> cls) {
		Class<?> c = cls;
		SerDes.As[] ann;
		while ((ann = c.getAnnotationsByType(SerDes.As.class)).length == 0 && (null != (c = c.getSuperclass())));
		return ann;
	}

	static Format def(Class<?> rawClass) {
		// TODO Auto-generated method stub
		return null;
	}

	static Format CONST_FORMAT = new ConstFormat();
}
