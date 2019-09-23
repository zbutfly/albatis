package net.butfly.albatis.io.ext;

import java.util.List;
import java.util.concurrent.Executors;
//import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.WrapOutput;

public class BatchOutput<M> extends WrapOutput<M, M> {
	private static final long serialVersionUID = -3223283732451449278L;
	private LinkedBlockingQueue<M> lbq = new LinkedBlockingQueue<>(50000);
	private int size;
//	private static final ForkJoinPool EXECUTE_POOL_CA = //
//            new ForkJoinPool(Integer.parseInt(Configs.gets("albatis.batch.output.paral", "5")), ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
	private static final ThreadPoolExecutor CACHED_THREAD_POOL = (ThreadPoolExecutor) Executors.newFixedThreadPool(Integer.parseInt(Configs.gets("albatis.batch.output.paral", String.valueOf(Runtime.getRuntime().availableProcessors()))));
	private long timeout = Long.parseLong(Configs.gets("albatis.batch.output.timeout", "30000"));
	private long threadPoolWaitNum = Long.parseLong(Configs.gets("albatis.batch.output.wait.num.max", "-1"));
	private AtomicBoolean stopped = new AtomicBoolean(false);
	private AtomicBoolean bool = new AtomicBoolean(false);
	private AtomicLong count = new AtomicLong();

	public BatchOutput(Output<M> base, int size) {
		super(base, "_BATCH");
		this.size = Integer.parseInt(Configs.gets("albatis.batch.output.size", String.valueOf(size)));
		new Thread(new TimeTask()).start();
		new Thread(new WorkTask()).start();
	}

	@Override
	public void enqueue(Sdream<M> items) {
		items.eachs(m -> {
			try {
				lbq.put(m);
				count.getAndIncrement();
			} catch (InterruptedException e) {
				logger().error("put item into queue error" + m.toString(), e);
			}
		});
	}

	public class WorkTask implements Runnable {
		@Override
		public void run() {
			while (!stopped.get()) {
				List<M> batch = Colls.list();
				if (lbq.size() < size) {
					if (!bool.get()) {
						continue;
					} else {
						bool.set(false);
					}
				}
				lbq.drainTo(batch, size);
				if (batch.size() > 0) {
					while (threadPoolWaitNum >= 0 && CACHED_THREAD_POOL.getQueue().size() >= threadPoolWaitNum) {
						logger().debug("Waiting tasks are too many, waiting!");
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {}
					}
					CACHED_THREAD_POOL.submit(() -> base.enqueue(Sdream.of(batch)));
					count.getAndAdd(batch.size() * -1);
				}
			}
		}
	}

	public class TimeTask implements Runnable {
		private long last = 0l;
		public TimeTask() {
			last = System.currentTimeMillis();
		}

		@Override
		public void run() {
			while (!stopped.get()) {
				if (lbq.size() < size) {
					long now = System.currentTimeMillis();
					if (now - last > timeout) {
						last = now;
						bool.set(true);
					}
				} else
					last = System.currentTimeMillis();
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
			}
		}

	}

	@Override
	public void close() {
		while (!lbq.isEmpty() && count.get() != 0) {
			logger().info("batch cache is not empty, waiting 10s");
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
			}
		}
		CACHED_THREAD_POOL.shutdown();
		stopped.set(true);
		base.close();
	}
}
