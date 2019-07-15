package net.butfly.albatis.io.ext;

import static net.butfly.albatis.io.IOProps.STATS_STEP;
import static net.butfly.albatis.io.IOProps.propL;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.WrapOutput;

public class StatsOutput<V> extends WrapOutput<V, V> {
	StatsOutput(Output<V> base, String suffix) {
		super(base, "Stats" + suffix);
	}

	public StatsOutput(Output<V> base) {
		this(base, "");
	}

	private static final long serialVersionUID = 7269336981213958715L;
	private final long statsStep = propL(b(), STATS_STEP, -1);

	@Override
	public void enqueue(Sdream<V> items) {
		s().statsOuts(items.list(), ll -> base.enqueue(Sdream.of(ll)));
	}

	@Override
	public Statistic statistic() {
		Statistic s = base.statistic();
		if (statsStep > 0) s.step(statsStep);
		return s;
	}

	@Override
	public Statistic statistic(Qualifier t) {
		Statistic s = base.statistic();
		if (statsStep > 0) s.step(statsStep);
		return s;
	}
}
