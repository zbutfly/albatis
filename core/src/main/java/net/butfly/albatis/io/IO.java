package net.butfly.albatis.io;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.io.Openable;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;

public interface IO extends Sizable, Openable {
	class Props {
		static final String BATCH_SIZE = "batch.size";
		static final String STATS_STEP = "stats.step";
		static Map<IO, Map<String, Number>> PROPS = Maps.of();

		public static String prop(Class<?> io, String suffix, String def) {
			return Configs.gets("albatis." + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_DOT, io.getSimpleName()) + "." + suffix, def);
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

	@Override
	default void open() {
		Map<String, Number> props = Props.PROPS.computeIfAbsent(this, io -> Maps.of());
		long step = props.computeIfAbsent(Props.STATS_STEP, k -> Props.propL(getClass(), k, -1)).longValue();
		logger().info("Step set to [" + step + "]");
		s().step(step);
		Openable.super.open();
	}

	default <E> Sdream<E> stats(Sdream<E> s) {
		return s.peek(e -> s().stats(e));
	}

	default int batchSize() {
		return Props.PROPS.computeIfAbsent(this, io -> Maps.of()).computeIfAbsent(Props.BATCH_SIZE, //
				k -> Props.propI(getClass(), k, 500)).intValue();
	}

	class Stats {
		private static final Map<IO, Statistic> IO_STATS = new ConcurrentHashMap<>();
	}

	default Statistic s() {
		return Stats.IO_STATS.computeIfAbsent(this, IO::trace);
	}

	default void statistic(Statistic s) {
		if (null == s) Stats.IO_STATS.remove(this);
		else Stats.IO_STATS.put(this, s);
	}
}
