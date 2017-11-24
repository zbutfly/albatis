package net.butfly.albatis.io;

import static net.butfly.albacore.paral.Task.waitSleep;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.steam.Steam;

public abstract class OddOutput<V> extends Namedly implements Output<V> {
	protected OddOutput() {
		super();
	}

	protected OddOutput(String name) {
		super(name);
	}

	protected abstract boolean enqueue(V v);

	@Override
	public final void enqueue(Steam<V> s) {
		if (!waitSleep(() -> full())) return;
		s.each(v -> {
			if (enqueue(v)) succeeded(1);
		});
	}
}
