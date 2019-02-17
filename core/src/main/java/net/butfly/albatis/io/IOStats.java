package net.butfly.albatis.io;

import static net.butfly.albatis.io.IOProps.propL;
import static net.butfly.albatis.io.IOProps.STATS_STEP;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albacore.utils.logger.Statistic;

public interface IOStats extends Loggable {
	static final Map<IOStats, Statistic> IO_STATS = new ConcurrentHashMap<>();

	/**
	 * default disable stats, if inherited and return a valid {@code Statistic}, enable statis on this io instnce.
	 */
	default Statistic trace() {
		return new Statistic(this);
	}

	default Statistic stating() {
		return s().step(propL(this, STATS_STEP, -1));
	}

	default <E> Sdream<E> stats(Sdream<E> s) {
		return s.peek(e -> s().stats(e));
	}

	class Stats {}

	default Statistic s() {
		return IO_STATS.computeIfAbsent(this, IOStats::trace);
	}

	default void statistic(Statistic s) {
		if (null == s) IO_STATS.remove(this);
		else IO_STATS.put(this, s);
	}
}
