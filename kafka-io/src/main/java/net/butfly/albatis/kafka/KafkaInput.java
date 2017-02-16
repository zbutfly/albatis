package net.butfly.albatis.kafka;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.OpenableThread;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.lambda.Consumer;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.async.Concurrents;

public final class KafkaInput extends KafkaInputBase<KafkaInput.Fetcher> {
	private IBigQueue pool;

	public KafkaInput(String name, final String kafkaURI, final String poolPath, String... topics) throws ConfigException, IOException {
		super(name, kafkaURI, topics);
		init(poolPath);
	}

	public KafkaInput(String name, URISpec kafkaURI, String poolPath) throws ConfigException {
		super(name, kafkaURI);
		init(poolPath);
	}

	private void init(String poolPath) {
		try {
			pool = new BigQueueImpl(IOs.mkdirs(poolPath + "/" + name), config.toString());
		} catch (IOException e) {
			throw new RuntimeException("Offheap pool init failure", e);
		}
		logger.info(MessageFormat.format("[{0}] local pool init: [{1}/{0}] with name [{2}], init size [{3}].", name, poolPath, config
				.toString(), pool.size()));
		for (String t : raws.keySet()) {
			AtomicInteger i = new AtomicInteger(0);
			for (KafkaStream<byte[], byte[]> stream : raws.get(t)) {
				streams.compute(t, (k, v) -> {
					Map<KafkaStream<byte[], byte[]>, Fetcher> v1 = v == null ? new HashMap<>() : v;
					Fetcher f = new Fetcher(name + "Fetcher#" + t, stream, i.incrementAndGet(), pool, config.getPoolSize());
					f.start();
					logger.trace("[" + name + "] fetcher [" + i.get() + "] started at topic [" + t + "].");
					v1.put(stream, f);
					return v1;
				});
			}
		}
		open();
	}

	@Override
	protected KafkaMessage fetch(KafkaStream<byte[], byte[]> stream, Fetcher fetcher, Consumer<KafkaMessage> result) {
		KafkaMessage m = fetch();
		result.accept(m);
		return m;
	}

	private KafkaMessage fetch() {
		byte[] buf;
		try {
			buf = pool.dequeue();
		} catch (IOException e) {
			return null;
		}
		if (null == buf) return null;
		return new KafkaMessage(buf);
	}

	@Override
	public Stream<KafkaMessage> dequeue(long batchSize, Iterable<String> keys) {
		try {
			return super.dequeue(batchSize, keys);
		} finally {
			gc(pool);
		}
	}

	private static void gc(IBigQueue pool) {
		try {
			pool.gc();
		} catch (IOException e) {
			logger.warn("Local pool gc() failure", e);
		}
	}

	@Override
	public void close() {
		super.close(this::closePool);
	}

	private void closePool() {
		try {
			pool.gc();
		} catch (IOException e) {
			logger.error("[" + name() + "] local pool gc failure", e);
		}
		try {
			pool.close();
		} catch (IOException e) {
			logger.error("[" + name() + "] local pool close failure", e);
		}
	}

	public long poolSize() {
		return pool.size();
	}

	static class Fetcher extends OpenableThread {
		private final KafkaStream<byte[], byte[]> stream;
		private final IBigQueue pool;
		private final long poolSize;

		public Fetcher(String inputName, KafkaStream<byte[], byte[]> stream, int i, IBigQueue pool, long poolSize) {
			super(inputName + "#" + i);
			this.stream = stream;
			this.pool = pool;
			this.poolSize = poolSize;
			this.setUncaughtExceptionHandler((t, e) -> {
				logger.error("[" + getName() + "] async error, pool [" + pool.size() + "]", e);
			});
		}

		@Override
		protected void exec() {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (opened())
				try {
					while (opened() && it.hasNext()) {
						byte[] km = new KafkaMessage(it.next()).toBytes();
						while (opened() && pool.size() > poolSize)
							Concurrents.waitSleep();
						pool.enqueue(km);
					}
					sleep(1000); // kafka empty
				} catch (Exception e) {
					// logger.warn("Consumer fetching failure", e);
				} finally {
					gc(pool);
				}
			logger.info("Fetcher finished and exited, pool [" + pool.size() + "].");
		}
	}
}
