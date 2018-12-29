package net.butfly.albatis.io.ext;

import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.OpenableThread;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Queue;
import net.butfly.albatis.io.WrapInput;

public class PrefetchInput<V> extends WrapInput<V, V> {
	private static final long serialVersionUID = 6202521008850298132L;
	private final Queue<V> pool;
	private final OpenableThread prefetching;

	public PrefetchInput(Input<V> base, Queue<V> pool) {
		super(base, "Prefetch");
		this.pool = pool;
		prefetching = new OpenableThread(() -> {
			while (opened() && !base.empty())
				base.dequeue(pool::enqueue);
		}, base.name() + "Prefetching");
		closing(() -> {
			prefetching.close();
			pool.close();
		});
		pool.open();
		prefetching.open();
	}

	@Override
	public void dequeue(Consumer<Sdream<V>> using) {
		pool.dequeue(using);
	}
}
