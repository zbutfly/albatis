package net.butfly.albatis.io.ext;

import static net.butfly.albatis.io.IOProps.STATS_SAMPLE_STEP;
import static net.butfly.albatis.io.IOProps.propL;

import java.util.List;

import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.io.Output;

public class SampleOutput<V> extends StatsOutput<V> {
	private static final long serialVersionUID = 2115147630344788786L;
	private final long sampleStep = propL(b(), STATS_SAMPLE_STEP, -1);
	private final Consumer<V> sampling;

	public SampleOutput(Output<V> base, Consumer<V> sampling) {
		super(base, "Sample");
		this.sampling = sampling;
	}

	@Override
	public void enqueue(Sdream<V> items) {
		List<V> l = items.list();
		try {
			base.enqueue(Sdream.of(l));
		} finally {
			Statistic s = s();
			l.forEach(s::stats);
		}
	}

	@Override
	public Statistic statistic() {
		Statistic s = super.statistic();
		if (sampleStep > 0) s.sampling(sampleStep, sampling);
		return s;
	}

	@Override
	public Statistic statistic(Qualifier t) {
		Statistic s = super.statistic(t);
		if (sampleStep > 0) s.sampling(sampleStep, sampling);
		return s;
	}
}
