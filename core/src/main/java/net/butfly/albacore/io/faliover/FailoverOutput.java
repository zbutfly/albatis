package net.butfly.albacore.io.faliover;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
	public void open() {
		super.open();
		failover.open();
	}

	protected abstract long write(K key, Iterable<M> pkg) throws FailoverException;

	protected void commit(K key) {}

	@Override
	public final long enqueue(Stream<M> els) {
		return Streams.of(IO.collect(els, Collectors.groupingByConcurrent(e -> e.partition()))).parallel().mapToLong(e -> {
			int size = e.getValue().size();
			if (size <= batchSize) return failover.output(e.getKey(), e.getValue());
			else {
				Stream<Iterator<M>> s = Streams.batch((size + 1) / batchSize, e.getValue().iterator());
				return s.parallel().mapToLong(it -> failover.output(e.getKey(), () -> it)).sum();
			}
		}).sum();
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
