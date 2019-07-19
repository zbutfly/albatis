package net.butfly.albatis.io;

import static net.butfly.albatis.io.IOProps.STATS_STEP;
import static net.butfly.albatis.io.IOProps.propL;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.ddl.Qualifier;

public interface IOStats extends Loggable {
	static final Map<String, Statistic> IO_STATS = new ConcurrentHashMap<>();

	/**
	 * default disable stats, if inherited and return a valid {@code Statistic}, enable stats on this io instance.
	 */
	default Statistic statistic() {
		return new Statistic(this);
	}

	default Statistic statistic(Qualifier t) {
		long s = propL(this, STATS_STEP, -1);
		Statistic ss = statistic();
		if (s > 0) ss.step(s);
		return ss;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	default <R extends IO> R b() {
		return (R) ((this instanceof Wrapper) ? ((Wrapper) this).bases() : this);
	}

	default Statistic s() {
		return IO_STATS.computeIfAbsent(b().getClass().getName(), ln -> statistic()).step(propL(this, STATS_STEP, -1));
	}

	default void statistic(Statistic s) {
		if (null == s) IO_STATS.remove(b().getClass().getName());
		else IO_STATS.put(b().getClass().getName(), s);
	}
}
