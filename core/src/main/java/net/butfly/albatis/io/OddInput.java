package net.butfly.albatis.io;

import java.util.function.Consumer;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.Sdream;

public abstract class OddInput<V> extends Namedly implements Input<V> {
	protected OddInput() {
		super();
	}

	protected OddInput(String name) {
		super(name);
	}

	protected abstract V dequeue();

	@Override
	public final void dequeue(Consumer<Sdream<V>> using, int batchSize) {
		using.accept(Sdream.of(this::dequeue, batchSize, () -> empty() && opened()));
	}
}
