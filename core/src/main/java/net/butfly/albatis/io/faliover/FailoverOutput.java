package net.butfly.albatis.io.faliover;

import static net.butfly.albacore.utils.collection.Streams.of;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.EnqueueException;
import net.butfly.albacore.utils.parallel.Concurrents;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.faliover.Failover.BytesConstruct;
import net.butfly.albatis.io.faliover.Failover.BytesDestruct;

/**
 * Output with buffer and failover supporting.<br>
 * Parent class handle buffer, invoking really write/marshall op by callback provided by children classes.<br>
 * Children classes define and implemented connection to datasource.
 * 
 * @author zx
 *
 * @param <M>
 */
public class FailoverOutput<M> extends Namedly implements Output<M> {
	private final Failover<M> failover;
	protected final AtomicLong actionCount = new AtomicLong(0);
	private final int batchSize;
	private final Output<M> output;

	protected FailoverOutput(Output<M> output, BytesConstruct<M> construct, BytesDestruct<M> destruct) throws IOException {
		super(output.name() + "WithHeapFailover");
		this.output = output;
		batchSize = Integer.parseInt(System.getProperty("albatis.io.failover.batch", "1000"));
		failover = Failover.create(output, construct, destruct, batchSize);
		failover.trace(batchSize, () -> "failover: " + failover.size());
		closing(() -> {
			long act;
			while ((act = actionCount.longValue()) > 0 && Concurrents.waitSleep())
				logger().info("Failover Async op waiting for closing: " + act);
			failover.close();
		});
	}

	protected FailoverOutput(Output<M> output, BytesConstruct<M> construct, BytesDestruct<M> destruct, String failoverPath)
			throws IOException {
		super(output.name() + "WithOffHeapFailover");
		this.output = output;
		batchSize = Integer.parseInt(System.getProperty("albatis.io.failover.batch", "1000"));
		failover = Failover.create(output, construct, destruct, batchSize, failoverPath);
		failover.trace(batchSize, () -> "failover: " + failover.size());
		closing(() -> {
			long act;
			while ((act = actionCount.longValue()) > 0 && Concurrents.waitSleep())
				logger().info("Failover Async op waiting for closing: " + act);
			failover.close();
		});
	}

	@Override
	public final long enqueue(Stream<M> els) {
		try {
			return output.enqueue(els);
		} catch (EnqueueException ex) {
			failover.enqueue(of(ex.fails()));
			return ex.success();
		}
	}

	public final long fails() {
		return failover.size();
	}

	@Override
	public boolean opened() {
		return output.opened();
	}
}