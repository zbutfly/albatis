package net.butfly.albatis.io.pump;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import net.butfly.albacore.io.Openable;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.stats.Statistical;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.ext.FanOutput;

public interface Pump<V> extends Statistical<Pump<V>>, Openable {
	Pump<V> batch(int batchSize);

	@Override
	default void open() {
		Openable.super.open();
		Systems.handleSignal(sig -> {
			close();
			System.err.println("Maybe you need to kill me manually: kill -9 " + Systems.pid() + "\n" + Systems.threadsRunning().count()
					+ " threads remain: ");
			System.err.println("\t" + Systems.threadsRunning().map(t -> t.getId() + "[" + t.getName() + "]").collect(Collectors.joining(
					", ")));
		}, "TERM", "INT");
	}

	public static <V> Pump<V> pump(Input<V> input, int parallelism, Output<V> dest) {
		return new BasicPump<V>(input, parallelism, dest);
	}

	@SafeVarargs
	public static <V> Pump<V> pump(Input<V> input, int parallelism, Output<V>... dests) {
		return new BasicPump<>(input, parallelism, new FanOutput<V>(Arrays.asList(dests)));
	}

	public static <V> Pump<V> pump(Input<V> input, int parallelism, List<? extends Output<V>> dests) {
		return new BasicPump<>(input, parallelism, new FanOutput<V>(dests));
	}
}
