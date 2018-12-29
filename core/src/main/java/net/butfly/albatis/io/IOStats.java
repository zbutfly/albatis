package net.butfly.albatis.io;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albacore.utils.logger.Statistic;

public interface IOStats extends Loggable {
	class Props {
		static final String BATCH_SIZE = "batch.size";
		static final String STATS_STEP = "stats.step";
		static Map<IOStats, Map<String, Number>> PROPS = Maps.of();
		static final Map<IOStats, Statistic> IO_STATS = new ConcurrentHashMap<>();

		public static String propName(Class<?> io, String suffix) {
			return "albatis." + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_DOT, io.getSimpleName()) + "." + suffix;
		}

		private static String propDefName(Class<?> io, String suffix) {
			String name = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_DOT, io.getSimpleName());
			if (name.endsWith(".input")) name = "*.input";
			else if (name.endsWith(".output")) name = "*.output";
			else if (name.endsWith(".queue")) name = "*.queue";
			else name = "*";
			return "albatis." + name + "." + suffix;
		}

		public static String prop(Class<?> io, String suffix, String def) {
			return Configs.gets(propName(io, suffix), Configs.gets(propDefName(io, suffix), def));
		}

		public static long propL(Class<?> io, String suffix, long def) {
			return Long.parseLong(prop(io, suffix, Long.toString(def)));
		}

		public static int propI(Class<?> io, String suffix, int def) {
			return Integer.parseInt(prop(io, suffix, Integer.toString(def)));
		}

		public static boolean propB(Class<?> io, String suffix, boolean def) {
			return Boolean.parseBoolean(prop(io, suffix, Boolean.toString(def)));
		}
	}

	/**
	 * default disable stats, if inherited and return a valid {@code Statistic}, enable statis on this io instnce.
	 */
	default Statistic trace() {
		return new Statistic(this);
	}

	default void stating() {
		Map<String, Number> props = Props.PROPS.computeIfAbsent(this, io -> Maps.of());
		IO b = Wrapper.bases((IO) this);
		long step = props.computeIfAbsent(Props.STATS_STEP, k -> Props.propL(b.getClass(), k, -1)).longValue();
		logger().debug(() -> "Step set to [" + step + "] by [" + Props.propName(b.getClass(), Props.STATS_STEP) + "]");
		s().step(step);
	}

	default <E> Sdream<E> stats(Sdream<E> s) {
		return s.peek(e -> s().stats(e));
	}

	class Stats {}

	default Statistic s() {
		return Props.IO_STATS.computeIfAbsent(this, IOStats::trace);
	}

	default void statistic(Statistic s) {
		if (null == s) Props.IO_STATS.remove(this);
		else Props.IO_STATS.put(this, s);
	}
}
