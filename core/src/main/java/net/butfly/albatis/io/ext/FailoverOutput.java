package net.butfly.albatis.io.ext;

import static net.butfly.albacore.utils.collection.Streams.of;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import net.butfly.albacore.io.EnqueueException;
import net.butfly.albacore.utils.OpenableThread;
import net.butfly.albacore.utils.parallel.Concurrents;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Queue;
import net.butfly.albatis.io.Wrapper;

/**
 * Output with buffer and fails supporting.<br>
 * Parent class handle buffer, invoking really write/marshall op by callback provided by children classes.<br>
 * Children classes define and implemented connection to datasource.
 * 
 * @author zx
 *
 * @param <M>
 */
public class FailoverOutput<M> extends Wrapper.WrapOutput<M, M> {
	protected final Queue<M> fails;
	protected final OpenableThread failovering;
	protected final AtomicLong actionCount = new AtomicLong(0);

	public FailoverOutput(Output<M> output, Queue<M> failover, int batchSize) {
		super(output, "Failover");
		this.fails = failover;
		failovering = new OpenableThread(() -> {
			while (opened())
				failover.dequeue(ms -> output.enqueue(ms), batchSize);
		}, "Failovering");
		opening(() -> {
			fails.open();
			failovering.open();
		});
		closing(() -> {
			failovering.close();
			fails.close();
			long act;
			while ((act = actionCount.longValue()) > 0 && Concurrents.waitSleep())
				logger().info("Failover Async op waiting for closing: " + act);
		});
	}

	@Override
	public final long enqueue(Stream<M> els) {
		try {
			return base.enqueue(els);
		} catch (EnqueueException ex) {
			fails.enqueue(of(ex.fails()));
			return ex.success();
		}
	}

	public final long fails() {
		return fails.size();
	}

	@Override
	public void close() {
		super.close();
	}

	@Override
	public String toString() {
		return super.toString() + "[fails: " + fails() + "]";
	}
}