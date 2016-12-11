package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.MapInput;
import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.lambda.Consumer;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.config.KafkaInputConfig;
import net.butfly.albatis.kafka.config.Kafkas;

abstract class KafkaInputBase<T> extends MapInput<String, KafkaMessage> {
	private static final long serialVersionUID = -9180560064999614528L;
	protected static final Logger logger = Logger.getLogger(Kafka2Input.class);

	private final ConsumerConnector connect;
	private final Map<String, Map<KafkaStream<byte[], byte[]>, T>> streams;

	public KafkaInputBase(String name, final String config, String... topic) throws ConfigException, IOException {
		super(name);
		KafkaInputConfig kic = new KafkaInputConfig(config);
		logger.info("KafkaInput [" + name() + "] connecting with config [" + kic.toString() + "].");
		Map<String, Integer> topics = new HashMap<>();
		int kp = kic.getPartitionParallelism();
		for (Entry<String, Integer> info : Kafkas.getTopicInfo(kic.getZookeeperConnect(), topic).entrySet()) {
			if (kp <= 0) topics.put(info.getKey(), 1);
			else if (kp >= info.getValue()) topics.put(info.getKey(), info.getValue());
			else topics.put(info.getKey(), (int) Math.ceil(info.getValue() * 1.0 / kp));
		}

		logger.debug("KafkaInput [" + name() + "] parallelism of topics: " + topics.toString() + ".");
		connect = kafka.consumer.Consumer.createJavaConsumerConnector(kic.getConfig());
		Map<String, List<KafkaStream<byte[], byte[]>>> s = connect.createMessageStreams(topics);

		streams = parseStreams(s, kic.getPoolSize());
		logger.debug("KafkaInput [" + name() + "] connected.");
	}

	protected abstract Map<String, Map<KafkaStream<byte[], byte[]>, T>> parseStreams(Map<String, List<KafkaStream<byte[], byte[]>>> streams,
			long poolSize);

	protected abstract KafkaMessage fetch(KafkaStream<byte[], byte[]> stream, T lock, Consumer<KafkaMessage> result);

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
		for (Map<KafkaStream<byte[], byte[]>, T> tm : streams.values())
			for (T f : tm.values())
				if (f instanceof AutoCloseable) try {
					((AutoCloseable) f).close();
				} catch (Exception e) {
					logger.error("KafkaInput [" + name() + "] internal fetcher close failure", e);
				}
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