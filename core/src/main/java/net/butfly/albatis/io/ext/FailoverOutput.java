package net.butfly.albatis.io.ext;

import static net.butfly.albacore.utils.collection.Streams.of;

import java.util.stream.Stream;

import net.butfly.albacore.io.EnqueueException;
import net.butfly.albacore.utils.OpenableThread;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Queue;
import net.butfly.albatis.io.Wrapper;

/**
 * Output with buffer and pool supporting.<br>
 * Parent class handle buffer, invoking really write/marshall op by callback provided by children classes.<br>
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
			while (opened())
				failover.dequeue(ms -> output.enqueue(ms), batchSize);
		}, "Failovering");
		closing(() -> {
			failovering.close();
			pool.close();
		});
		pool.open();
		open();
		failovering.open();
	}

	@Override
	public final long enqueue(Stream<M> els) {
		try {
			return base.enqueue(els);
		} catch (EnqueueException ex) {
			pool.enqueue(of(ex.fails()));
			return ex.success();
		}
	}

	public final long fails() {
		return pool.size();
	}

	@Override
	public String toString() {
		return super.toString() + "[fails: " + fails() + "]";
	}
}