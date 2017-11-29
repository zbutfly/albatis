package net.butfly.albatis.io.ext;

import java.util.function.Consumer;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.OpenableThread;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Queue;
import net.butfly.albatis.io.Wrapper;

public class PrefetchInput<V> extends Wrapper.WrapInput<V, V> {
	private final Queue<V> pool;
	private final OpenableThread prefetching;

	public PrefetchInput(Input<V> base, Queue<V> pool, int batchSize) {
		super(base, "Prefetch");
		this.pool = pool;
		prefetching = new OpenableThread(() -> {
			while (opened() && !base.empty())
				base.dequeue(pool::enqueue, batchSize);
		}, base.name() + "Prefetching");
		closing(() -> {
			prefetching.close();
			pool.close();
		});
		pool.open();
		open();
		prefetching.open();
	}

	@Override
	public void dequeue(Consumer<Sdream<V>> using, int batchSize) {
		pool.dequeue(using, batchSize);
	}
}
