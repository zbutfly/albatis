package net.butfly.albacore.io.faliover;

import static net.butfly.albacore.io.utils.Streams.list;
import static net.butfly.albacore.io.utils.Streams.of;
import static net.butfly.albacore.utils.IOs.mkdirs;
import static net.butfly.albacore.utils.IOs.writeBytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.bluejeans.bigqueue.BigQueue;

import net.butfly.albacore.io.BigqQueue;
import net.butfly.albacore.io.Message;
import net.butfly.albacore.io.utils.Streams;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.parallel.Concurrents;
import net.butfly.albacore.utils.parallel.Parals;

public class OffHeapFailover extends Failover {
	private BigQueue failover;

	public OffHeapFailover(String parentName, FailoverOutput output, String path, String poolName) throws IOException {
		this(parentName, output, Message.class, path, poolName);
	}

	public OffHeapFailover(String parentName, FailoverOutput output, Class<? extends Message> messageClass, String path, String poolName)
			throws IOException {
		super(parentName, output, messageClass);
		if (poolName == null) poolName = "POOL";
		failover = new BigQueue(mkdirs(path + "/" + parentName), poolName);
		logger.debug(MessageFormat.format("Failover [persist mode] init: [{0}/{1}] with name [{2}], init size [{3}].", //
				path, parentName, poolName, size()));
		closing(this::closePool);
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
	protected final void exec() {
		while (opened()) {
			while (opened() && failover.isEmpty())
				Concurrents.waitSleep(1000);
			Pair<String, List<Message>> results;
			while (null != (results = Parals.run(this::fetch))) {
				Pair<String, List<Message>> r = results;
				Parals.run(() -> output(r.v1(), stats(of(r.v2()))));
			}
		}
	}

	private Pair<String, List<Message>> fetch() throws IOException {
		byte[] bytes;
		if (!opened() || null == (bytes = failover.dequeue()) || 0 == bytes.length) return null;
		try (ByteArrayInputStream baos = new ByteArrayInputStream(bytes);) {
			String k = IOs.readObj(baos);
			List<Message> values = Streams.collect(Streams.of(IOs.readBytesList(baos)).map(b -> {
				if (null == b) return null;
				try {
					return construct.newInstance(b);
				} catch (Exception ex) {
					return null;
				}
			}), Collectors.toList());
			return values.isEmpty() ? null : new Pair<>(k, values);
		}
	}

	@Override
	public long size() {
		return failover.size();
	}

	@Override
	public boolean isEmpty() {
		return failover.isEmpty();
	}

	@Override
	protected long fail(String key, Collection<? extends Message> values) {
		try {
			byte[] bytes;
			try (ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
				IOs.writeObj(baos, key);
				writeBytes(baos, list(values, Message::toBytes).toArray(new byte[values.size()][]));
				bytes = baos.toByteArray();
			}
			if (bytes.length == 0) return 0;
			failover.enqueue(bytes);
			return values.size();
		} catch (IOException e) {
			logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}]", values.size(), key), e);
			return 0;
		}
	}

	private void closePool() {
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