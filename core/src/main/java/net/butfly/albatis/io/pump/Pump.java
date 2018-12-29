package net.butfly.albatis.io.pump;

import static net.butfly.albacore.utils.Systems.handleSignal;
import static net.butfly.albacore.utils.Systems.pid;
import static net.butfly.albacore.utils.Systems.threadsRunning;

import java.util.List;

import net.butfly.albacore.io.Openable;

public interface Pump<V> extends Openable {
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
}
