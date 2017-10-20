package net.butfly.albatis.io;

import static net.butfly.albacore.utils.collection.Streams.map;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.parallel.Concurrents;

public interface OddOutput<V> extends Output<V> {
	boolean enqueue(V v);

	@Override
	public default void enqueue(Sdream<V> items) {
		if (!waitWhen(() -> full())) return;
		items.eachs(v -> {
			if (enqueue(v)) succeeded(1);
			else failed(v);
		});
	}

	@Override
	public final void enqueue(Stream<V> items) {
		if (!Concurrents.waitSleep(() -> full())) return;
		succeeded(map(items, t -> enqueue(t) ? 1 : 0, Collectors.summingLong(l -> l)));
	}

	default void failed(V v) {}
}
