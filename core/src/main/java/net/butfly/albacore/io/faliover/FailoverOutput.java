package net.butfly.albacore.io.faliover;

import java.io.IOException;
import java.util.Collection;
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
public abstract class FailoverOutput<M extends Message<String, ?, M>> extends OutputImpl<M> {
	private final Failover<String, M> failover;

	protected FailoverOutput(String name, Function<byte[], ? extends M> constructor, String failoverPath, int packageSize, int parallelism)
			throws IOException {
		super(name);
		if (failoverPath == null) failover = new HeapFailover<String, M>(name(), kvs -> write(kvs._1, kvs._2), this::commit, constructor,
				packageSize, parallelism);
		else failover = new OffHeapFailover<String, M>(name(), kvs -> write(kvs._1, kvs._2), this::commit, constructor, failoverPath, null,
				packageSize, parallelism) {};
	}

	@Override
	public void open(java.lang.Runnable run) {
		super.open(run);
		failover.open();
	}

	protected abstract int write(String key, Collection<M> values) throws FailoverException;

	protected void commit(String key) {}

	@Override
	public final boolean enqueue(M e) {
		return enqueue(Stream.of(e)) == 1;
	}

	@Override
	public final long enqueue(Stream<M> els) {
		return failover.enqueueTask(io.collect(Streams.of(els), Collectors.groupingBy(e -> e.partition(), Collectors.mapping(t -> t,
				Collectors.toList()))));
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

	public final int tasks() {
		return failover.tasks();
	}
}
