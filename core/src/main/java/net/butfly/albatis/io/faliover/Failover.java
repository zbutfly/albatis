package net.butfly.albatis.io.faliover;

import static net.butfly.albacore.utils.IOs.mkdirs;
import static net.butfly.albacore.utils.IOs.writeBytes;
import static net.butfly.albacore.utils.collection.Streams.list;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.bluejeans.bigqueue.BigQueue;

import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.OpenableThread;
import net.butfly.albacore.utils.collection.Streams;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.parallel.Concurrents;
import net.butfly.albacore.utils.parallel.Parals;
import net.butfly.albacore.utils.stats.Statistical;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Queue;
import net.butfly.albatis.io.ext.BigqQueue;

public abstract class Failover<M> extends OpenableThread implements Queue<M>, Statistical<Failover<M>> {
	private static final Logger logger = Logger.getLogger(Failover.class);
	private static final int MAX_FAILOVER = 50000;
	protected final Output<M> output;
	protected final BytesConstruct<M> construct;
	protected final BytesDestruct<M> destruct;
	protected final int batchSize;

	public interface BytesConstruct<M> {
		M construct(byte[] data) throws IOException;
	}

	public interface BytesDestruct<M> {
		byte[] destruct(M msg) throws IOException;
	}

	public static <M> Failover<M> create(Output<M> output, BytesConstruct<M> construct, BytesDestruct<M> destruct, int batchSize) {
		return new HeapFailover<>(output, construct, destruct, batchSize);
	}

	public static <M> Failover<M> create(Output<M> output, BytesConstruct<M> construct, BytesDestruct<M> destruct, int batchSize,
			String path) throws IOException {
		return new OffHeapFailover<>(output, construct, destruct, batchSize, path, "failover");
	}

	protected Failover(Output<M> output, BytesConstruct<M> construct, BytesDestruct<M> destruct, int batchSize) {
		super(output.name() + "Failover");
		this.output = output;
		this.construct = construct;
		this.destruct = destruct;
		this.batchSize = batchSize;
	}

	@Override
	protected void exec() {
		while (output.opened())
			dequeue(s -> Parals.run(() -> output.enqueue(stats(s))), batchSize);
		if (!empty()) logger().warn("Failover closed, data remain: " + size() + ".");
	}

	@Override
	public void close() {
		while (output.opened()) // confirm!! NO lost!!
			Concurrents.waitSleep(100, logger(), "output not closed, waiting!");
	}

	private static class HeapFailover<M> extends Failover<M> {
		private BlockingQueue<M> failover = new LinkedBlockingQueue<>(MAX_FAILOVER);

		HeapFailover(Output<M> output, BytesConstruct<M> construct, BytesDestruct<M> destruct, int batchSize) {
			super(output, construct, destruct, batchSize);
			logger.info(MessageFormat.format("Output [{0}] failover [memory mode] init.", output.name()));
			open();
		}

		@Override
		public long size() {
			return failover.size();
		}

		@Override
		public long enqueue(Stream<M> values) {
			AtomicLong c = new AtomicLong();
			Stream<M> vs = values.filter(Streams.NOT_NULL);
			if (opened()) {
				vs.forEach(m -> {
					if (null != m) try {
						failover.put(m);
						c.incrementAndGet();
					} catch (IllegalStateException | InterruptedException ee) {
						logger.error("Failover failed, doc lost: " + m.toString());
					}
				});
				return c.get();
			} else {
				logger().error(vs.map(m -> m.toString()).collect(Collectors.joining("\n\t", "Failover closed, data lost: \n\t",
						"\n========")));
				return 0;
			}
		}

		@Override
		public long dequeue(Function<Stream<M>, Long> using, int batchSize) {
			List<M> retries = new ArrayList<>(batchSize);
			failover.drainTo(retries, batchSize);
			if (retries.isEmpty()) return 0;
			return using.apply(Streams.of(retries));
		}

		@Override
		public void close() {
			while (output.opened()) // confirm!! NO lost!!
				Concurrents.waitSleep(100, logger(), "output not closed, waiting!");
		}
	}

	private static class OffHeapFailover<M> extends Failover<M> {
		private BigQueue failover;

		OffHeapFailover(Output<M> output, BytesConstruct<M> construct, BytesDestruct<M> destruct, int batchSize, String path,
				String poolName) throws IOException {
			super(output, construct, destruct, batchSize);
			if (poolName == null) poolName = "CACHE";
			failover = new BigQueue(mkdirs(path + "/" + output.name()), poolName);
			logger.debug(MessageFormat.format("Failover [persist mode] init: [{0}/{1}] with name [{2}], init size [{3}].", //
					path, output.name(), poolName, size()));
			open();
			Thread gc = new Thread(() -> {
				do {
					try {
						failover.gc();
					} catch (Throwable t) {
						logger().error("Pool gc fail", t);
					}
				} while (opened() && Concurrents.waitSleep(BigqQueue.GC_INTV));
			}, "Failover-Maintainancer-Daemon-Thread");
			gc.setDaemon(true);
			gc.start();
		}

		@Override
		public long size() {
			return failover.size();
		}

		@Override
		public long enqueue(Stream<M> values) {
			AtomicLong c = new AtomicLong();
			Stream<M> vs = values.filter(Streams.NOT_NULL);
			if (opened()) {
				byte[] bytes;
				try (ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
					writeBytes(baos, list(vs, v -> {
						try {
							return destruct.destruct(v);
						} catch (IOException e) {
							return null;
						}
					}).toArray(new byte[0][]));
					bytes = baos.toByteArray();
				} catch (IOException e) {
					return 0;
				}
				if (bytes.length == 0) return 0;
				failover.enqueue(bytes);

				return c.get();
			} else {
				logger().error(vs.map(m -> m.toString()).collect(Collectors.joining("\n\t", "Failover closed, data lost: \n\t",
						"\n========")));
				return 0;
			}
		}

		@Override
		public long dequeue(Function<Stream<M>, Long> using, int batchSize) {
			List<M> retries = fetch();
			if (null == retries || retries.isEmpty()) return 0;
			return using.apply(Streams.of(retries));
		}

		private List<M> fetch() {
			byte[] bytes;
			if (!opened() || null == (bytes = failover.dequeue()) || 0 == bytes.length) return null;
			try (ByteArrayInputStream baos = new ByteArrayInputStream(bytes);) {
				List<M> values = Streams.collect(Streams.of(IOs.readBytesList(baos)).map(b -> {
					if (null == b) return null;
					try {
						return construct.construct(b);
					} catch (Exception ex) {
						return null;
					}
				}), Collectors.toList());
				return values.isEmpty() ? null : values;
			} catch (IOException e) {
				return null;
			}
		}

		@Override
		public void close() {
			while (output.opened()) // confirm!! NO lost!!
				Concurrents.waitSleep(100, logger(), "output not closed, waiting!");
			failover.gc();
			long size = size();
			try {
				failover.close();
			} catch (IOException e) {
				logger.error("Failover close failure", e);
			} finally {
				if (size > 0) logger.error("Failover closed and remained [" + size + "].");
			}
		}
	}
}
