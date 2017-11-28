package net.butfly.albatis.io.pump;

import static net.butfly.albacore.utils.Systems.handleSignal;
import static net.butfly.albacore.utils.Systems.pid;
import static net.butfly.albacore.utils.Systems.threadsRunning;

import java.util.Arrays;
import java.util.List;

import net.butfly.albacore.io.Openable;
import net.butfly.albacore.utils.stats.Statistical;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.ext.FanOutput;

public interface Pump<V> extends Statistical<Pump<V>>, Openable {
	Pump<V> batch(int batchSize);

	@Override
	default void open() {
		Openable.super.open();
		handleSignal(sig -> {
			close();
			System.err.println("Maybe you need to kill me manually: kill -9 " + pid() + "\n" + threadsRunning().count()
					+ " threads remain: ");
			System.err.println("\t" + threadsRunning().joinAsString(t -> t.getId() + "[" + t.getName() + "]", ","));
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
