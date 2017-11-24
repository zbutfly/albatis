package net.butfly.albatis.io;

import java.util.function.Consumer;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.steam.Steam;

public abstract class OddInput<V> extends Namedly implements Input<V> {
	protected OddInput() {
		super();
	}

	protected OddInput(String name) {
		super(name);
	}

	protected abstract V dequeue();

	@Override
	public final void dequeue(Consumer<Steam<V>> using, int batchSize) {
		using.accept(Steam.of(this::dequeue, batchSize, () -> empty() && opened()));
	}
}
