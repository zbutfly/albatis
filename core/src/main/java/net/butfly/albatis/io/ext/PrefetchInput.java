package net.butfly.albatis.io.ext;

import java.util.function.Function;
import java.util.stream.Stream;

import net.butfly.albacore.utils.OpenableThread;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Queue;
import net.butfly.albatis.io.Wrapper.WrapInput;

class PrefetchInput<V> extends WrapInput<V> {
	private final Queue<V> pool;
	private final OpenableThread fetcher;

	PrefetchInput(Input<V> base, Queue<V> pool, int fetchSize) {
		super(base);
		this.pool = pool;
		fetcher = new OpenableThread(() -> fetch(base, fetchSize), base.name() + "PrefetcherThread");
		closing(fetcher::close);
	}

	private void fetch(Input<V> base, int fetchSize) {
		while (opened() && !base.empty())
			base.dequeue(pool::enqueue, fetchSize);
	}

	@Override
	public long dequeue(Function<Stream<V>, Long> using, int batchSize) {
		return pool.dequeue(using, batchSize);
	}
}
