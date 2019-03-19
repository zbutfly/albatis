package net.butfly.albatis.io;

import static net.butfly.albacore.utils.Configs.getss;

import java.util.Map;

import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.collection.Maps;

public interface IOProps {
	static final String BATCH_SIZE = "batch.size";
	static final String STATS_STEP = "stats.step";
	static Map<Object, Map<String, Object>> PROPS = Maps.of();

	static String propName(Class<?> c, String suffix) {
		if (null == c) return null;
		else return "albatis." + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_DOT, c.getSimpleName()) + "." + suffix;
	}

	class _Priv {
		private static String defaultPropName(Class<?> c, String suffix) {
			if (null == c) return null;
			String name = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_DOT, c.getSimpleName());
			if (name.endsWith(".input")) name = "*.input";
			else if (name.endsWith(".output")) name = "*.output";
			else if (name.endsWith(".queue")) name = "*.queue";
			else name = "*";
			return "albatis." + name + "." + suffix;
		}

		private static Class<?> unwrap(Object io) {
			if (null == io) return null;
			if (io instanceof Class) return (Class<?>) io;
			Class<?> c = io.getClass();
			return IO.class.isAssignableFrom(c) ? Wrapper.bases((IO) io).getClass() : c;
		}

		private static Map<String, Object> props(Object io) {
			return PROPS.computeIfAbsent(io, obj -> Maps.of());
		}

		private static String prop(Object io, String suffix, String def) {
			Class<?> c = _Priv.unwrap(io);
			String k = propName(c, suffix);
			return getss(null, def, k, _Priv.defaultPropName(c, suffix));
		}

		private static String prop(Object io, String suffix, String def, String... comments) {
			Class<?> c = _Priv.unwrap(io);
			String k = propName(c, suffix);
			return getss(comments.length == 0 ? null : String.join(" ", comments), def, k, _Priv.defaultPropName(c, suffix));
		}
	}

	static String prop(Object io, String suffix, long def, String... comments) {
		return ((CharSequence) _Priv.props(io).computeIfAbsent(suffix, //
				k -> Long.parseLong(_Priv.prop(io, suffix, Long.toString(def), comments)))).toString();
	}

	static long propL(Object io, String suffix, long def, String... comments) {
		return ((Number) _Priv.props(io).computeIfAbsent(suffix, //
				k -> Long.parseLong(_Priv.prop(io, suffix, Long.toString(def), comments)))).longValue();
	}

	static int propI(Object io, String suffix, int def, String... comments) {
		return ((Number) _Priv.props(io).computeIfAbsent(suffix, //
				k -> Integer.parseInt(_Priv.prop(io, suffix, Integer.toString(def), comments)))).intValue();
	}

	static boolean propB(Object io, String suffix, boolean def, String... comments) {
		return ((Boolean) _Priv.props(io).computeIfAbsent(suffix, //
				k -> Boolean.parseBoolean(_Priv.prop(io, suffix, Boolean.toString(def), comments)))).booleanValue();
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
