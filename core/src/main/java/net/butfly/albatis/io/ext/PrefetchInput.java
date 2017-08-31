package net.butfly.albatis.io.ext;

import java.util.function.Function;
import java.util.stream.Stream;

import net.butfly.albacore.utils.OpenableThread;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Queue;
import net.butfly.albatis.io.Wrapper;

public class PrefetchInput<V> extends Wrapper.WrapInput<V, V> {
	private final Queue<V> prefetch;
	private final OpenableThread fetcher;
	private final Queue<V> pool;

	public PrefetchInput(Input<V> base, Queue<V> pool, int batchSize) {
		super(base, "Prefetch");
		this.prefetch = pool;
		this.pool = pool;
		fetcher = new OpenableThread(() -> {
			while (opened() && !base.empty())
				base.dequeue(prefetch::enqueue, batchSize);
		}, base.name() + "PrefetcherThread");
		closing(fetcher::close);
	}

	@Override
	public long dequeue(Function<Stream<V>, Long> using, int batchSize) {
		return prefetch.dequeue(using, batchSize);
	}

	@Override
	public void open() {
		pool.open();
		super.open();
		prefetch.open();
	}

	@Override
	public void close() {
		prefetch.close();
		super.close();
		pool.close();
	}
}
