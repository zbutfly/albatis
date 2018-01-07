package net.butfly.albatis.io;

import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.io.Openable;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.logger.Statistical;

public interface IO extends Sizable, Openable, Statistical {
	static final String BATCH_SIZE = "batch.size";
	static final String STATS_STEP = "stats.step";

	default <E> Sdream<E> stats(Sdream<E> s) {
		return s.peek(this::stats);
	}

	default String prop(String suffix, String def) {
		return Configs.gets("albatis." + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_DOT, getClass().getSimpleName()) + "." + suffix, def);
	}

	default long propL(String suffix, long def) {
		return Long.parseLong(prop(suffix, Long.toString(def)));
	}

	default int propI(String suffix, int def) {
		return Integer.parseInt(prop(suffix, Integer.toString(def)));
	}

	default boolean propB(String suffix, boolean def) {
		return Boolean.parseBoolean(prop(suffix, Boolean.toString(def)));
	}

	default long statsStep() {
		return propL(STATS_STEP, -1);
	}

	default int batchSize() {
		return propI(BATCH_SIZE, 500);
	}
}
