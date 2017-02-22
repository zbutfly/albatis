package net.butfly.albacore.io.faliover;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.Message;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.async.Concurrents;

public class OffHeapFailover<K, V extends Message<K, ?, V>> extends Failover<K, V> {
	private IBigQueue failover;

	public OffHeapFailover(String parentName, FailoverOutput<K, V> output, Function<byte[], ? extends V> constructor, String path,
			String poolName) throws IOException {
		super(parentName, output, constructor);
		if (poolName == null) poolName = "POOL";
		failover = new BigQueueImpl(IOs.mkdirs(path + "/" + parentName), poolName);
		logger.info(MessageFormat.format("Failover [persist mode] init: [{0}/{1}] with name [{2}], init size [{3}].", //
				path, parentName, poolName, size()));
	}

	@Override
	protected void exec() {
		while (opened()) {
			while (opened() && failover.isEmpty())
				Concurrents.waitSleep(1000);
			while (opened() && !failover.isEmpty()) {
				Pair<K, List<V>> results;
				try {
					results = IO.io.run(this::fetch);
				} catch (Exception e) {
					continue;
				}
				IO.io.run(() -> output(results.v1(), results.v2()));
				stats(results.v2());
			}
		}
	}

	private Pair<K, List<V>> fetch() throws IOException {
		Pair<K, byte[][]> results = fromBytes(failover.dequeue());
		return new Pair<>(results.v1(), IO.io.list(Arrays.asList(results.v2()), b -> construct.apply(b)));
	}

	@Override
	public long size() {
		return failover.size();
	}

	@Override
	public boolean isEmpty() {
		return failover.isEmpty();
	}

	private byte[] toBytes(K key, Collection<V> values) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos);) {
			oos.writeObject(key);
			IOs.writeInt(baos, values.size());
			IOs.writeBytes(baos, IO.io.list(values, v -> v.toBytes()).toArray(new byte[values.size()][]));
			return baos.toByteArray();
		}
	}

	private Pair<K, byte[][]> fromBytes(byte[] data) throws IOException {
		try (ByteArrayInputStream baos = new ByteArrayInputStream(data); ObjectInputStream oos = new ObjectInputStream(baos);) {
			@SuppressWarnings("unchecked")
			K k = (K) oos.readObject();
			int c = IOs.readInt(baos);
			byte[][] results = new byte[c][];
			for (int i = 0; i < c; i++)
				results[i] = IOs.readBytes(baos);
			return new Pair<>(k, results);
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void fail(K key, Collection<V> values, Exception err) {
		try {
			failover.enqueue(toBytes(key, values));
			if (null != err) logger.warn(MessageFormat.format(
					"Failure added on [{0}] with [{1}] docs, now [{2}] failover on [{0}], caused by [{3}]", //
					key, values.size(), size(), err.getMessage()));
		} catch (IOException e) {
			if (null != err) logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}], original caused by [{2}]", //
					values.size(), key, err.getMessage()), e);
			else logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}]", values.size(), key), e);
			return;
		}
	}

	@Override
	public void close(Runnable run) {
		super.close(this::closePool);
	}

	private void closePool() {
		try {
			failover.gc();
		} catch (IOException e) {
			logger.error("Failover cleanup failure", e);
		}
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