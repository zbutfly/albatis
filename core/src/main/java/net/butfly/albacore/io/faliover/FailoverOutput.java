package net.butfly.albacore.io.faliover;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
	final int packageSize;

	protected FailoverOutput(String name, Function<byte[], ? extends M> constructor, String failoverPath, int packageSize)
			throws IOException {
		super(name);
		this.packageSize = packageSize;
		failover = failoverPath == null ? new HeapFailover<K, M>(name(), this, constructor)
				: new OffHeapFailover<K, M>(name(), this, constructor, failoverPath, null);
	}

	@Override
	public void open(java.lang.Runnable run) {
		super.open(run);
		failover.open();
	}

	protected abstract int write(K key, Collection<M> values) throws FailoverException;

	protected void commit(K key) {}

	@Override
	public final boolean enqueue(M e) {
		return enqueue(Stream.of(e)) == 1;
	}

	@Override
	public final long enqueue(Stream<M> els) {
		Map<K, List<M>> map = io.collect(Streams.of(els), Collectors.groupingBy(e -> e.partition(), Collectors.mapping(t -> t, Collectors
				.toList())));
		AtomicLong count = new AtomicLong(0);
		io.each(map.entrySet(), e -> count.addAndGet(failover.output(e.getKey(), e.getValue())));
		return count.get();
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
