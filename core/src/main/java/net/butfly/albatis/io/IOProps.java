package net.butfly.albatis.io;

import java.util.Map;

import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Maps;

public interface IOProps {
	static final String BATCH_SIZE = "batch.size";
	static final String STATS_STEP = "stats.step";
	static Map<Object, Map<String, Object>> PROPS = Maps.of();

	static String propName(Object io, String suffix) {
		if (null == io) return null;
		else return "albatis." + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_DOT, io.getClass().getSimpleName()) + "." + suffix;
	}

	class _Priv {
		private static String defaultPropName(Object io, String suffix) {
			if (null == io) return null;
			String name = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_DOT, io.getClass().getSimpleName());
			if (name.endsWith(".input")) name = "*.input";
			else if (name.endsWith(".output")) name = "*.output";
			else if (name.endsWith(".queue")) name = "*.queue";
			else name = "*";
			return "albatis." + name + "." + suffix;
		}

		private static Object unwrap(Object io) {
			if (null == io) return null;
			else return io instanceof IO ? Wrapper.bases((IO) io).getClass() : io.getClass();

		}

		private static Map<String, Object> props(Object io) {
			return PROPS.computeIfAbsent(io, obj -> Maps.of());
		}

		private static String prop(Object io, String suffix, String def) {
			Object obj = _Priv.unwrap(io);
			String k = propName(obj, suffix);
			String v = Configs.gets(k);
			return null == v ? Configs.gets(_Priv.defaultPropName(obj, suffix), def) : v;
		}
	}

	static String prop(Object io, String suffix, long def) {
		return ((CharSequence) _Priv.props(io).computeIfAbsent(suffix, //
				k -> Long.parseLong(_Priv.prop(io, suffix, Long.toString(def))))).toString();
	}

	static long propL(Object io, String suffix, long def) {
		return ((Number) _Priv.props(io).computeIfAbsent(suffix, //
				k -> Long.parseLong(_Priv.prop(io, suffix, Long.toString(def))))).longValue();
	}

	static int propI(Object io, String suffix, int def) {
		return ((Number) _Priv.props(io).computeIfAbsent(suffix, //
				k -> Integer.parseInt(_Priv.prop(io, suffix, Integer.toString(def))))).intValue();
	}

	static boolean propB(Object io, String suffix, boolean def) {
		return ((Boolean) _Priv.props(io).computeIfAbsent(suffix, //
				k -> Boolean.parseBoolean(_Priv.prop(io, suffix, Boolean.toString(def))))).booleanValue();
	}
}
