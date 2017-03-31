package net.butfly.albacore.io.faliover;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.Message;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.io.utils.Parals;
import net.butfly.albacore.io.utils.Streams;

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
public abstract class FailoverOutput<K, M extends Message<K, ?, M>> extends Namedly implements Output<M> {
	private final Failover<K, M> failover;
	final int batchSize;

	protected FailoverOutput(String name, Function<byte[], ? extends M> constructor, String failoverPath, int batchSize)
			throws IOException {
		super(name);
		this.batchSize = batchSize;
		failover = failoverPath == null ? new HeapFailover<K, M>(name(), this, constructor)
				: new OffHeapFailover<K, M>(name(), this, constructor, failoverPath, null);
		failover.trace(batchSize, () -> "failover: " + size());
		closing(this::closeInternal);
	}

	protected abstract long write(K key, Stream<M> pkg, Consumer<Collection<M>> failing, Consumer<Long> committing, int retry);

	protected void commit(K key) {}

	@Override
	public final long enqueue(Stream<M> els) {
		Map<K, List<M>> parts = IO.collect(els, Collectors.groupingByConcurrent(M::partition));
		if (parts.isEmpty()) return 0;
		return eachs(parts.entrySet(), e -> enqueue(e.getKey(), e.getValue()), Streams.LONG_SUM);
	}

	private long enqueue(K key, List<M> items) {
		if (null == items || items.isEmpty()) return 0;
		int size = items.size();
		return size <= batchSize ? failover.output(key, Streams.of(items))
				: eachs(Streams.spatial(items.spliterator(), Parals.calcParallelism(size, batchSize)).values(), it -> failover.output(key,
						Streams.of(it)), Streams.LONG_SUM);

	}

	protected abstract void closeInternal();

	@Override
	public void close() {
		Output.super.close();
		failover.close();
	}

	public final long fails() {
		return failover.size();
	}

	@Override
	public boolean opened() {
		return Output.super.opened() && failover.opened();
	}
}
