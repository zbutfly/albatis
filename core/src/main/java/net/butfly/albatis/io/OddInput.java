package net.butfly.albatis.io;

import java.util.function.Consumer;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.steam.Sdream;

public abstract class OddInput<V> extends Namedly implements Input<V> {
	protected OddInput() {
		super();
	}

	@Override
	public default void dequeue(Consumer<Sdream<V>> using) {
		using.accept(Sdream.of(new SupSpliterator<>(this, () -> empty() || !opened(), batchSize())));
	}

	class SupSpliterator<V> implements Spliterator<V> {
		private int est;
		private final OddInput<V> input;

	@Override
	public final void dequeue(Consumer<Sdream<V>> using, int batchSize) {
		using.accept(Sdream.of(this::dequeue, batchSize, () -> empty() && opened()));
	}
}
