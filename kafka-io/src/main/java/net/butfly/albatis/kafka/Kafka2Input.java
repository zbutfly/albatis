package net.butfly.albatis.kafka;

import java.io.IOException;
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

public class Kafka2Input extends KafkaInputBase<Kafka2Input.KafkaInputFetcher> {
	private static final long serialVersionUID = 1813167082084278062L;
	private IBigQueue pool;

	public Kafka2Input(String name, final String config, final String poolPath, String... topic) throws ConfigException, IOException {
		super(name, config, topic);
		this.pool = new BigQueueImpl(poolPath, conf.toString());
	}

	@Override
	protected KafkaMessage fetch(KafkaStream<byte[], byte[]> stream, KafkaInputFetcher fetcher, Consumer<KafkaMessage> result) {
		try {
			KafkaMessage m = new KafkaMessage(pool.dequeue());
			result.accept(m);
			return m;
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	protected Map<String, Map<KafkaStream<byte[], byte[]>, KafkaInputFetcher>> parseStreams(
			Map<String, List<KafkaStream<byte[], byte[]>>> s, long poolSize) {
		Map<String, Map<KafkaStream<byte[], byte[]>, KafkaInputFetcher>> ss = new HashMap<>();
		AtomicInteger i = new AtomicInteger(0);
		for (String t : s.keySet())
			for (KafkaStream<byte[], byte[]> stream : s.get(t)) {
				ss.compute(t, (k, v) -> {
					Map<KafkaStream<byte[], byte[]>, KafkaInputFetcher> v1 = v == null ? new HashMap<>() : v;
					KafkaInputFetcher f = new KafkaInputFetcher(name(), stream, i.incrementAndGet(), pool);
					logger.info("KafkaInput [" + name() + "] fetcher [" + i.get() + "] started.");
					f.start();
					v1.put(stream, f);
					return v1;
				});
			}
		return ss;
	}

	static final class KafkaInputFetcher extends Thread implements AutoCloseable {
		public AtomicBoolean closed = new AtomicBoolean(false);
		private final KafkaStream<byte[], byte[]> stream;
		private final IBigQueue pool;

		public KafkaInputFetcher(String inputName, KafkaStream<byte[], byte[]> stream, int i, IBigQueue pool) {
			this.stream = stream;
			this.pool = pool;
			this.setName("Kafka2InputFetcher-" + i);
			this.setUncaughtExceptionHandler((t, e) -> {
				logger.error("KafkaInput [" + inputName + "] fetcher [" + t.getName() + "] error, pool size: [" + pool.size() + "].", e);
			});
		}

		@Override
		public void run() {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (!closed.get())
				try {
					while (it.hasNext())
						pool.enqueue(new KafkaMessage(it.next()).toBytes());
				} catch (Exception e) {}
		}

		@Override
		public void close() {
			closed.set(true);
			interrupt();
		}
	}
}
