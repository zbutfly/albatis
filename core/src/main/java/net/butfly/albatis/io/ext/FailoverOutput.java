package net.butfly.albatis.io.ext;

import java.util.stream.Stream;

import net.butfly.albacore.utils.OpenableThread;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Queue;
import net.butfly.albatis.io.Wrapper;

/**
 * Output with buffer and pool supporting.<br>
 * Parent class handle buffer, invoking really write/marshall op by callback
 * provided by children classes.<br>
 * Children classes define and implemented connection to datasource.
 * 
 * @author zx
 *
 * @param <M>
 */
public class FailoverOutput<M> extends Wrapper.WrapOutput<M, M> {
	protected final Queue<M> pool;
	protected final OpenableThread failovering;

	public FailoverOutput(Output<M> output, Queue<M> failover, int batchSize) {
		super(output, "Failover");
		this.pool = failover;
		failovering = new OpenableThread(() -> {
			while (output.opened() && opened())
				failover.dequeue(output::enqueue, batchSize);
			logger().info("Failovering stopped.");
		}, name() + "Failovering");
		closing(() -> {
			failovering.close();
			if (!pool.empty()) logger().warn("Failovering pool not empty [" + pool.size() + "], maybe lost.");
			else logger().info("Failovering pool empty and closing.");
			pool.close();
		});
		pool.open();
		open();
		failovering.open();
	}

	@Override
	public final void enqueue(Stream<M> els) {
		base.enqueue(els);
	}

	@Override
	public void failed(Stream<M> failed) {
		pool.enqueue(failed);
	}

	public final long fails() {
		return pool.size();
	}

	@Override
	public String toString() {
		return super.toString() + "[fails: " + fails() + "]";
	}
}