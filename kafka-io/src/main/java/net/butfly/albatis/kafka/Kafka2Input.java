package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.lambda.Consumer;

public class Kafka2Input extends KafkaInputBase<Kafka2Input.KafkaInputFetcher> {
	private static final long serialVersionUID = 1813167082084278062L;

	public Kafka2Input(String name, final String config, String... topic) throws ConfigException, IOException {
		super(name, config, (s, i) -> {
			KafkaInputFetcher t = new KafkaInputFetcher(s, i);
			t.start();
			return t;
		}, topic);
	}

	@Override
	protected KafkaMessage fetch(KafkaStream<byte[], byte[]> stream, KafkaInputFetcher fetcher, Consumer<KafkaMessage> result) {
		KafkaMessage m = fetcher.pool.poll();
		if (null != m) result.accept(m);
		return m;
	}

	static final class KafkaInputFetcher extends Thread {
		private final LinkedBlockingQueue<KafkaMessage> pool;
		private final KafkaStream<byte[], byte[]> stream;

		public KafkaInputFetcher(KafkaStream<byte[], byte[]> stream, int i) {
			pool = new LinkedBlockingQueue<>(10000);
			this.stream = stream;
			this.setName("Kafka2InputFetcher-" + i);
			this.setUncaughtExceptionHandler((t, e) -> {
				logger.error("Fetcher [" + t.getName() + "] error, pool size: [" + ((KafkaInputFetcher) t).pool.size() + "].", e);
			});
		}

		@Override
		public void run() {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (true)
				try {
					while (it.hasNext()) {
						pool.put(new KafkaMessage(it.next()));
					}
				} catch (Exception e) {}
		}
	}

	@Override
	public long poolStatus() {
		long count = 0;
		for (Map<KafkaStream<byte[], byte[]>, KafkaInputFetcher> m : streams.values())
			for (KafkaInputFetcher p : m.values())
				count += p.pool.size();
		return count;
	}
}
