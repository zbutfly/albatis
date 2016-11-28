package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.KafkaProducer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.MapInput;
import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.config.KafkaInputConfig;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

public class Kafka2Input extends MapInput<String, KafkaMessage> {
	private static final long serialVersionUID = 7617065839861658802L;
	private static final Logger logger = Logger.getLogger(Kafka2Input.class);
	private final ConsumerConnector connect;
	private final Map<String, Integer> topics;
	private final Map<String, Map<KafkaStream<byte[], byte[]>, Prefetcher>> streams;

	public Kafka2Input(String name, final String config, String... topic) throws ConfigException, IOException {
		super(name);
		KafkaInputConfig kic = new KafkaInputConfig(config);
		topics = new HashMap<>();
		if (kic.isParallelismEnable()) try (KafkaProducer<byte[], byte[]> kc = new KafkaProducer<>(new KafkaOutputConfig(config)
				.props());) {
			for (String t : topic) {
				int c = kc.partitionsFor(t).size();
				logger.info("Topic [" + t + "] partitions detected: " + c);
				topics.put(t, c);
			}
		}
		else for (String t : topic) {
			int c = 2;
			logger.info("Topic [" + t + "] partitions default: " + c);
			topics.put(t, c);
		}

		logger.debug("Kafka [" + config.toString() + "] connecting.");
		connect = Consumer.createJavaConsumerConnector(kic.getConfig());
		logger.debug("Kafka [" + config.toString() + "] Connected.");
		streams = parseStreams(connect.createMessageStreams(topics));
		logger.debug("KafkaInput ready in [" + streams.size() + "] topics: " + topics.toString());
	}

	@Override
	public KafkaMessage dequeue0(String topic) {
		Map<KafkaStream<byte[], byte[]>, Prefetcher> iters = streams.get(topic);
		for (KafkaStream<byte[], byte[]> stream : Collections.disorderize(iters.keySet())) {
			KafkaMessage e = iters.get(stream).get();
			if (null != e) return e;
		}
		return null;
	}

	@Override
	public List<KafkaMessage> dequeue(long batchSize, String... topic) {
		List<KafkaMessage> batch = new ArrayList<>();
		long prev;
		try {
			do {
				prev = batch.size();
				for (String t : topic) {
					Map<KafkaStream<byte[], byte[]>, Prefetcher> iters = streams.get(t);
					for (KafkaStream<byte[], byte[]> stream : Collections.disorderize(iters.keySet())) {
						KafkaMessage e = iters.get(stream).get();
						if (null != e) batch.add(e);
					}
				}
				if (batch.size() >= batchSize) return batch;
				if (batch.size() == 0) Concurrents.waitSleep(EMPTY_WAIT_MS);
			} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
			return batch;
		} finally {
			connect.commitOffsets(true);
		}
	}

	@Override
	public List<KafkaMessage> dequeue(long batchSize) {
		return dequeue(batchSize, keys().toArray(new String[0]));
	}

	@Override
	public boolean empty(String key) {
		return false;
	}

	@Override
	public long size() {
		return Long.MAX_VALUE;
	}

	@Override
	public long size(String key) {
		return Long.MAX_VALUE;
	}

	@Override
	public Set<String> keys() {
		return streams.keySet();
	}

	@Override
	public Q<Void, KafkaMessage> q(String key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() {
		logger.debug("KafkaInput [" + name() + "] closing...");
		connect.commitOffsets(true);
		connect.shutdown();
		logger.info("KafkaInput [" + name() + "] closed.");
	}

	private Map<String, Map<KafkaStream<byte[], byte[]>, Prefetcher>> parseStreams(Map<String, List<KafkaStream<byte[], byte[]>>> streams) {
		Map<String, Map<KafkaStream<byte[], byte[]>, Prefetcher>> s = new HashMap<>();
		AtomicInteger i = new AtomicInteger();
		for (String topic : streams.keySet())
			for (KafkaStream<byte[], byte[]> stream : streams.get(topic))
				s.compute(topic, (t, m) -> {
					Map<KafkaStream<byte[], byte[]>, Prefetcher> threads = m == null ? new HashMap<>() : m;
					Prefetcher p = new Prefetcher(stream, i.incrementAndGet());
					threads.put(stream, p);
					p.start();
					return threads;
				});
		return s;
	}

	private static final class Prefetcher extends Thread {
		private final LinkedBlockingQueue<KafkaMessage> pool;
		private final KafkaStream<byte[], byte[]> stream;

		public Prefetcher(KafkaStream<byte[], byte[]> stream, int i) {
			pool = new LinkedBlockingQueue<>(10000);
			this.stream = stream;
			this.setName("Kafka2InputFetcher-" + i);
			this.setUncaughtExceptionHandler((t, e) -> {
				logger.error("Fetcher [" + t.getName() + "] error, pool size: [" + ((Prefetcher) t).pool.size() + "].", e);
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
				} catch (Exception e) {
					// logger.error("Fetcher [" + getName() + "] error, pool
					// size: [" + pool.size() + "].", e);
				}
		}

		public KafkaMessage get() {
			return pool.poll();
		}
	}

	public long poolStatus() {
		long count = 0;
		for (Map<KafkaStream<byte[], byte[]>, Prefetcher> m : streams.values())
			for (Prefetcher p : m.values())
				count += p.pool.size();
		return count;
	}
}
