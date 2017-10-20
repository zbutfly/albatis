package net.butfly.albatis.io;

import static net.butfly.albacore.utils.collection.Streams.map;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.parallel.Concurrents;

public abstract class OddOutput<V> extends Namedly implements Output<V> {
	protected OddOutput() {
		super();
	}

	protected OddOutput(String name) {
		super(name);
	}

	protected abstract boolean enqueue(V item);

	@Override
	public final void enqueue(Stream<V> items) {
		if (!Concurrents.waitSleep(() -> full())) return;
		succeeded(map(items, t -> enqueue(t) ? 1 : 0, Collectors.summingLong(l -> l)));
	}
}
