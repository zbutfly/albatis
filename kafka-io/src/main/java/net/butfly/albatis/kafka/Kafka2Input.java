package net.butfly.albatis.kafka;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.lambda.Consumer;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.async.Concurrents;

public class Kafka2Input extends KafkaInputBase<Kafka2Input.KafkaInputFetcher> {
	private static final long serialVersionUID = 1813167082084278062L;
	private IBigQueue pool;

	public Kafka2Input(String name, final String config, final String poolPath, String... topic) throws ConfigException, IOException {
		super(name, config, topic);
		try {
			pool = new BigQueueImpl(IOs.mkdirs(poolPath + "/" + name), conf.toString());
		} catch (IOException e) {
			throw new RuntimeException("Offheap pool init failure", e);
		}
		logger.info(MessageFormat.format("KafkaInput [{0}] local cache init: [{1}/{2}] with name [{3}], init size [{4}].", name, poolPath,
				name, conf.toString(), pool.size()));
		AtomicInteger i = new AtomicInteger(0);
		for (String t : raws.keySet())
			for (KafkaStream<byte[], byte[]> stream : raws.get(t))
				streams.compute(t, (k, v) -> {
					Map<KafkaStream<byte[], byte[]>, KafkaInputFetcher> v1 = v == null ? new HashMap<>() : v;
					KafkaInputFetcher f = new KafkaInputFetcher(name, stream, i.incrementAndGet(), pool, conf.getPoolSize());
					logger.info("KafkaInput [" + name + "] fetcher [" + i.get() + "] started.");
					f.start();
					v1.put(stream, f);
					return v1;
				});
	}

	@Override
	protected KafkaMessage fetch(KafkaStream<byte[], byte[]> stream, KafkaInputFetcher fetcher, Consumer<KafkaMessage> result) {
		byte[] buf;
		try {
			buf = pool.dequeue();
		} catch (IOException e) {
			return null;
		}
		if (null == buf) return null;
		KafkaMessage m = new KafkaMessage(buf);
		result.accept(m);
		return m;
	}

	@Override
	public List<KafkaMessage> dequeue(long batchSize, String... topic) {
		try {
			return super.dequeue(batchSize, topic);
		} finally {
			// System.gc();
			try {
				pool.gc();
			} catch (IOException e) {
				logger.warn("KafkaInput [" + name() + "] local cache gc failure", e);
			}
		}
	}

	@Override
	public void close() {
		super.close();
		try {
			pool.close();
		} catch (IOException e) {
			logger.error("KafkaInput [" + name() + "] local cache close failure", e);
		}
	}

	public long poolSize() {
		return pool.size();
	}

	static final class KafkaInputFetcher extends Thread implements AutoCloseable {
		public AtomicBoolean closed = new AtomicBoolean(false);
		private final KafkaStream<byte[], byte[]> stream;
		private final IBigQueue pool;
		private final long poolSize;

		public KafkaInputFetcher(String inputName, KafkaStream<byte[], byte[]> stream, int i, IBigQueue pool, long poolSize) {
			this.stream = stream;
			this.pool = pool;
			this.poolSize = poolSize;
			this.setName(inputName + "-Fetcher-" + i);
			this.setUncaughtExceptionHandler((t, e) -> {
				logger.error("KafkaInputFetcher [" + getName() + "] async error, pool [" + pool.size() + "]", e);
			});
		}

		@Override
		public void run() {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (!closed.get())
				try {
					while (!closed.get() && it.hasNext()) {
						byte[] km = new KafkaMessage(it.next()).toBytes();
						while (pool.size() > poolSize)
							sleep(1000);
						pool.enqueue(km);
					}
					sleep(1000); // kafka empty
				} catch (Exception e) {
//					logger.warn("KafkaInputFetcher [" + getName() + "] failure, pool [" + pool.size() + "]", e);
				}
			logger.info("KafkaInputFetcher [" + getName() + "] finished, pool [" + pool.size() + "].");
		}

		@Override
		public void close() {
			closed.set(true);
			while (this.isAlive())
				Concurrents.waitSleep(100);
		}
	}
}
