package net.butfly.albatis.kafka;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.MapInput;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.lambda.Consumer;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.config.KafkaInputConfig;
import net.butfly.albatis.kafka.config.Kafkas;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

abstract class KafkaInputBase<T> extends MapInput<String, KafkaMessage> {
	private static final long serialVersionUID = -9180560064999614528L;
	protected static final Logger logger = Logger.getLogger(KafkaInputBase.class);
	protected final KafkaInputConfig conf;
	protected final Map<String, Integer> topics;
	protected final ConsumerConnector connect;
	protected final Map<String, List<KafkaStream<byte[], byte[]>>> raws;
	protected final Map<String, Map<KafkaStream<byte[], byte[]>, T>> streams;

	public KafkaInputBase(String name, final String kafkaURI, String... topic) throws ConfigException, IOException {
		super(name);
		conf = new KafkaInputConfig(new URISpec(kafkaURI));
		logger.info("[" + name() + "] connecting with config [" + conf.toString() + "].");
		topics = new HashMap<>();
		int kp = conf.getPartitionParallelism();
		for (Entry<String, Integer> info : Kafkas.getTopicInfo(conf.getZookeeperConnect(), topic).entrySet()) {
			if (kp <= 0) topics.put(info.getKey(), 1);
			else if (kp >= info.getValue()) topics.put(info.getKey(), info.getValue());
			else topics.put(info.getKey(), (int) Math.ceil(info.getValue() * 1.0 / kp));
		}

		logger.debug("[" + name() + "] parallelism of topics: " + topics.toString() + ".");
		connect = kafka.consumer.Consumer.createJavaConsumerConnector(conf.getConfig());
		raws = connect.createMessageStreams(topics);
		logger.debug("[" + name() + "] connected.");
		streams = new HashMap<>();
	}

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
	public void close() {
		super.close(this::closeKafka);
		Systems.disableGC();
	}

	private void closeKafka() {
		for (Map<KafkaStream<byte[], byte[]>, T> tm : streams.values())
			for (T f : tm.values())
				if (f instanceof AutoCloseable) try {
					((AutoCloseable) f).close();
				} catch (Exception e) {
					logger.error("[" + name() + "] internal fetcher close failure", e);
				}
		connect.commitOffsets(true);
		connect.shutdown();
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
			} while (opened() && batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
			return batch;
		} finally {
			connect.commitOffsets(true);
		}
	}
}
