package net.butfly.albatis.io.pump;

import static net.butfly.albacore.utils.Systems.handleSignal;
import static net.butfly.albacore.utils.Systems.pid;
import static net.butfly.albacore.utils.Systems.threadsRunning;

import java.util.Arrays;
import java.util.List;

import net.butfly.albacore.io.Openable;
import net.butfly.albacore.utils.collection.Colls;
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
			List<Thread> threads = threadsRunning().list();
			System.err.println("Maybe you need to kill me manually: kill -9 " + pid() + "\n" + threads.size() + " threads remain: ");
			int i = 0;
			for (Thread t : threads) {
				if (i++ < 10) System.err.println("\t" + t.getId() + "[" + t.getName() + "]");
				else {
					System.err.println("\t......................");
					break;
				}
			}
		}, "TERM", "INT");
	}

	public static <V> Pump<V> pump(Input<V> input, int parallelism, Output<V> dest) {
		return new BasicPump<V>(input, parallelism, dest.safe());
	}

	@SafeVarargs
	public static <V> Pump<V> pump(Input<V> input, int parallelism, Output<V>... dests) {
		return pump(input, parallelism, Arrays.asList(dests));
	}

	public static <V> Pump<V> pump(Input<V> input, int parallelism, List<? extends Output<V>> dests) {
		List<Output<V>> l = Colls.list();
		for (Output<V> o : dests)
			if (null != o && o.opened()) l.add(o.safe());
		return new BasicPump<>(input, parallelism, new FanOutput<V>(l));
	}
}
