package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.consumer.Consumer;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.MapInput;
import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.lambda.ConverterPair;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.config.KafkaInputConfig;
import net.butfly.albatis.kafka.config.Kafkas;

abstract class KafkaInputBase<T> extends MapInput<String, KafkaMessage> {
	private static final long serialVersionUID = -9180560064999614528L;
	protected static final Logger logger = Logger.getLogger(Kafka2Input.class);
	protected final ConsumerConnector connect;
	protected final Map<String, Integer> topics;
	protected final Map<String, Map<KafkaStream<byte[], byte[]>, T>> streams;

	public KafkaInputBase(String name, final String config, ConverterPair<KafkaStream<byte[], byte[]>, Integer, T> sync, String... topic)
			throws ConfigException, IOException {
		super(name);
		KafkaInputConfig kic = new KafkaInputConfig(config);
		logger.debug("Kafka [" + kic.toString() + "] connecting.");
		topics = new HashMap<>();
		if (kic.isParallelismEnable()) topics.putAll(Kafkas.getTopicInfo(kic.getZookeeperConnect(), topic));
		else for (String t : topic)
			topics.put(t, 1);
		logger.debug("Kafka [" + kic.toString() + "] parallelism: " + topics.toString() + ".");
		connect = Consumer.createJavaConsumerConnector(kic.getConfig());
		logger.debug("Kafka [" + kic.toString() + "] Connected.");
		Map<String, List<KafkaStream<byte[], byte[]>>> s = connect.createMessageStreams(topics);

		streams = new HashMap<>();
		AtomicInteger i = new AtomicInteger();
		for (String t : s.keySet())
			for (KafkaStream<byte[], byte[]> stream : s.get(t))
				streams.compute(t, (k, v) -> {
					Map<KafkaStream<byte[], byte[]>, T> v1 = v == null ? new HashMap<>() : v;
					v1.put(stream, sync.apply(stream, i.incrementAndGet()));
					return v1;
				});

		logger.info("KafkaInput " + name + " ready.");
	}

	protected abstract KafkaMessage fetch(KafkaStream<byte[], byte[]> stream, T lock,
			net.butfly.albacore.lambda.Consumer<KafkaMessage> result);

	public long poolStatus() {
		return -1;
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

	@Override
	public KafkaMessage dequeue0(String topic) {
		for (Entry<KafkaStream<byte[], byte[]>, T> s : Collections.disorderize(streams.get(topic).entrySet())) {
			KafkaMessage e = fetch(s.getKey(), s.getValue(), v -> connect.commitOffsets(false));
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
				for (String t : topic)
					for (Entry<KafkaStream<byte[], byte[]>, T> s : Collections.disorderize(streams.get(t).entrySet()))
						fetch(s.getKey(), s.getValue(), e -> batch.add(e));
				if (batch.size() >= batchSize) return batch;
				if (batch.size() == 0) Concurrents.waitSleep(EMPTY_WAIT_MS);
			} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
			return batch;
		} finally {
			connect.commitOffsets(true);
		}
	}
}
