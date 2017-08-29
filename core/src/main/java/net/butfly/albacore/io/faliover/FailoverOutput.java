package net.butfly.albacore.io.faliover;

import static net.butfly.albacore.io.utils.Streams.LONG_SUM;
import static net.butfly.albacore.io.utils.Streams.collect;
import static net.butfly.albacore.io.utils.Streams.of;
import static net.butfly.albacore.io.utils.Streams.spatial;
import static net.butfly.albacore.utils.parallel.Parals.calcBatchParal;
import static net.butfly.albacore.utils.parallel.Parals.eachs;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.Message;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.utils.parallel.Concurrents;

/**
 * Output with buffer and failover supporting.<br>
 * Parent class handle buffer, invoking really write/marshall op by callback provided by children classes.<br>
 * Children classes define and implemented connection to datasource.
 * 
 * @author zx
 *
 * @param <M>
 */
public abstract class FailoverOutput extends Namedly implements Output<Message> {
	private final Failover failover;
	public final int batchSize;
	protected final AtomicLong actionCount = new AtomicLong(0);

	protected FailoverOutput(String name, int batchSize) throws IOException {
		super(name);
		this.batchSize = batchSize;
		failover = new HeapFailover(name(), this);
		failover.trace(batchSize, () -> "failover: " + failover.size());
		closing(() -> {
			long act;
			while ((act = actionCount.longValue()) > 0 && Concurrents.waitSleep())
				logger().debug("ES Async op waiting for closing: " + act);
			failover.close();
			this.closeInternal();
		});
	}

	protected <M extends Message> FailoverOutput(String name, Class<M> mClass, String failoverPath, int batchSize) throws IOException {
		super(name);
		this.batchSize = batchSize;
		failover = new OffHeapFailover(name(), this, mClass, failoverPath, null);
		failover.trace(batchSize, () -> "failover: " + failover.size());
		closing(() -> {
			long act;
			while ((act = actionCount.longValue()) > 0 && Concurrents.waitSleep())
				logger().debug("ES Async op waiting for closing: " + act);
			failover.close();
			this.closeInternal();
		});
	}

	protected <M extends Message> FailoverOutput(String name, String failoverPath, int batchSize) throws IOException {
		this(name, Message.class, failoverPath, batchSize);
	}

	protected abstract <M extends Message> long write(String key, Stream<M> pkg, Consumer<Collection<M>> failing, Consumer<Long> committing,
			int retry);

	protected void commit(String key) {}

	@Override
	public final long enqueue(Stream<Message> els) {
		Map<String, List<Message>> parts = collect(els, Collectors.groupingByConcurrent(Message::table));
		if (parts.isEmpty()) return 0;
		return eachs(parts.entrySet(), e -> enqueue(e.getKey(), e.getValue()), LONG_SUM);
	}

	private long enqueue(String key, List<Message> items) {
		if (null == items || items.isEmpty()) return 0;
		int size = items.size();
		return size <= batchSize ? failover.output(key, of(items))
				: eachs(spatial(items.spliterator(), calcBatchParal(size, batchSize)).values(), it -> failover.output(key, of(it)),
						LONG_SUM);

	}

	protected abstract void closeInternal();

	public final long fails() {
		return failover.size();
	}

	@Override
	public boolean opened() {
		return Output.super.opened() && failover.opened();
	}
}