package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.KeyInputImpl;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.lambda.Consumer;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.config.KafkaInputConfig;
import net.butfly.albatis.kafka.config.Kafkas;

abstract class KafkaInputBase<V> extends KeyInputImpl<String, KafkaMessage> {
	protected static final Logger logger = Logger.getLogger(KafkaInputBase.class);
	protected final KafkaInputConfig config;
	protected final Map<String, Integer> allTopics;
	protected ConsumerConnector connect;
	protected final Map<String, List<KafkaStream<byte[], byte[]>>> raws;
	protected final Map<String, Map<KafkaStream<byte[], byte[]>, V>> streams;

	public KafkaInputBase(String name, final String kafkaURI, String[] topics) throws ConfigException, IOException {
		super(name);
		config = new KafkaInputConfig(new URISpec(kafkaURI));
		logger.info("[" + name() + "] connecting with config [" + config.toString() + "].");
		allTopics = new HashMap<>();
		int kp = config.getPartitionParallelism();
		for (Entry<String, Integer> info : Kafkas.getTopicInfo(config.getZookeeperConnect(), (topics != null && topics.length > 0)
				? new HashSet<>(Arrays.asList(topics)) : topics()).entrySet()) {
			if (kp <= 0) allTopics.put(info.getKey(), 1);
			else if (kp >= info.getValue()) allTopics.put(info.getKey(), info.getValue());
			else allTopics.put(info.getKey(), (int) Math.ceil(info.getValue() * 1.0 / kp));
		}

		logger.debug("[" + name() + "] parallelism of topics: " + allTopics.toString() + ".");
		connect = kafka.consumer.Consumer.createJavaConsumerConnector(config.getConfig());
		raws = connect.createMessageStreams(allTopics);
		logger.debug("[" + name() + "] connected.");
		streams = new HashMap<>();
	}

	protected abstract KafkaMessage fetch(KafkaStream<byte[], byte[]> stream, V lock, Consumer<KafkaMessage> result);

	@Override
	protected void readTo(String key, List<KafkaMessage> batch) {
		for (Entry<KafkaStream<byte[], byte[]>, V> s : Collections.disorderize(streams.get(key).entrySet()))
			fetch(s.getKey(), s.getValue(), e -> batch.add(e));
	}

	@Override
	protected void readCommit() {
		connect.commitOffsets(true);
	}

	@Override
	public Stream<KafkaMessage> dequeue(long batchSize) {
		return dequeue(batchSize, keys());
	}

	@Override
	public KafkaMessage dequeue() {
		String topic = Collections.disorderize(keys()).get(0);
		for (Entry<KafkaStream<byte[], byte[]>, V> s : Collections.disorderize(streams.get(topic).entrySet())) {
			KafkaMessage e = fetch(s.getKey(), s.getValue(), v -> connect.commitOffsets(false));
			if (null != e) return e;
		}
		return null;
	}

	@Override
	public KafkaMessage dequeue(String topic) {
		for (Entry<KafkaStream<byte[], byte[]>, V> s : Collections.disorderize(streams.get(topic).entrySet())) {
			KafkaMessage e = fetch(s.getKey(), s.getValue(), v -> connect.commitOffsets(false));
			if (null != e) return e;
		}
		return null;
	}

	@Override
	public Set<String> keys() {
		return streams.keySet();
	}

	@Override
	public void close() {
		super.close(this::closeKafka);
		Systems.disableGC();
	}

	private void closeKafka() {
		for (Map<KafkaStream<byte[], byte[]>, V> tm : streams.values())
			for (V f : tm.values())
				if (f instanceof AutoCloseable) try {
					((AutoCloseable) f).close();
				} catch (Exception e) {
					logger.error("[" + name() + "] internal fetcher close failure", e);
				}
		connect.commitOffsets(true);
		connect.shutdown();
		connect = null;
	}

	public Set<String> topics() {
		String[] topics = config.getTopics();
		return topics == null || topics.length == 0 ? new HashSet<>() : new HashSet<>(Arrays.asList(topics));
	}
}
