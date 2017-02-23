package net.butfly.albacore.io.faliover;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.ListenableFuture;

import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.Message;
import net.butfly.albacore.io.OutputImpl;
import net.butfly.albacore.io.Streams;
import net.butfly.albacore.io.faliover.Failover.FailoverException;

/**
 * Output with buffer and failover supporting.<br>
 * Parent class handle buffer, invoking really write/marshall op by callback
 * provided by children classes.<br>
 * Children classes define and implemented connection to datasource.
 * 
 * @author zx
 *
 * @param <M>
 */
public abstract class FailoverOutput<K, M extends Message<K, ?, M>> extends OutputImpl<M> {
	private final Failover<K, M> failover;
	final int batchSize;

	protected FailoverOutput(String name, Function<byte[], ? extends M> constructor, String failoverPath, int batchSize)
			throws IOException {
		super(name);
		this.batchSize = batchSize;
		failover = failoverPath == null ? new HeapFailover<K, M>(name(), this, constructor)
				: new OffHeapFailover<K, M>(name(), this, constructor, failoverPath, null);
		failover.trace(batchSize, () -> "failover: " + size());
	}

	@Override
	public void open(java.lang.Runnable run) {
		super.open(run);
		failover.open();
	}

	protected abstract int write(K key, Collection<M> values) throws FailoverException;

	protected void commit(K key) {}

	@Override
	public final long enqueue(Stream<M> els) {
		Iterator<M> it = els.iterator();
		List<ListenableFuture<Long>> fs = new ArrayList<>();
		while (it.hasNext()) {
			List<M> batch = Streams.batch(batchSize, it, null, false);
			fs.add(IO.listen(() -> {
				AtomicLong c = new AtomicLong(0);
				Map<K, List<M>> m = IO.collect(Streams.of(batch), Collectors.groupingBy(e -> e.partition(), Collectors.mapping(t -> t,
						Collectors.toList())));
				IO.each(m.entrySet(), e -> c.addAndGet(failover.output(e.getKey(), e.getValue())));
				return c.get();
			}));
		}
		return IO.sum(fs, logger());
	}

	@Override
	public final void close() {
		super.close();
		failover.close();
		closeInternal();
	}

	protected abstract void closeInternal();

	public final long fails() {
		return failover.size();
	}
}
