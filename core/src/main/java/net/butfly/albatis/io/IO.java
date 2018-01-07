package net.butfly.albatis.io;

import java.util.Map;

import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.io.Openable;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistical;

public interface IO extends Sizable, Openable, Statistical {
	class Props {
		static final String BATCH_SIZE = "batch.size";
		static final String STATS_STEP = "stats.step";
		static Map<IO, Map<String, Number>> PROPS = Maps.of();

		public static String prop(IO io, String suffix, String def) {
			return Configs.gets("albatis." + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_DOT, //
					io.getClass().getSimpleName()) + "." + suffix, def);
		}

		public static long propL(IO io, String suffix, long def) {
			return Long.parseLong(prop(io, suffix, Long.toString(def)));
		}

		public static int propI(IO io, String suffix, int def) {
			return Integer.parseInt(prop(io, suffix, Integer.toString(def)));
		}

		public static boolean propB(IO io, String suffix, boolean def) {
			return Boolean.parseBoolean(prop(io, suffix, Boolean.toString(def)));
		}
	}

	default <E> Sdream<E> stats(Sdream<E> s) {
		return s.peek(this::stats);
	}

	default long statsStep() {
		return Props.PROPS.computeIfAbsent(this, io -> Maps.of()).computeIfAbsent(Props.STATS_STEP, //
				k -> Props.propL(this, k, -1)).longValue();
	}

	default int batchSize() {
		return Props.PROPS.computeIfAbsent(this, io -> Maps.of()).computeIfAbsent(Props.STATS_STEP, //
				k -> Props.propI(this, Props.BATCH_SIZE, 500)).intValue();
	}
}
