//package net.butfly.albatis.kafka;
//
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//import java.util.Set;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.stream.Stream;
//
//import kafka.common.ConsumerRebalanceFailedException;
//import kafka.consumer.KafkaStream;
//import kafka.javaapi.consumer.ConsumerConnector;
//import net.butfly.albacore.exception.ConfigException;
//import net.butfly.albacore.io.KeyInputImpl;
//import net.butfly.albacore.io.URISpec;
//import net.butfly.albacore.lambda.Consumer;
//import net.butfly.albacore.utils.Texts;
//import net.butfly.albacore.utils.async.Concurrents;
//import net.butfly.albacore.utils.logger.Logger;
//import net.butfly.albatis.kafka.config.KafkaInputConfig;
//
//abstract class KafkaInputBase0<V> extends KeyInputImpl<String, KafkaMessage> {
//	protected static final Logger logger = Logger.getLogger(KafkaInputBase0.class);
//	protected KafkaInputConfig config;
//	protected final Map<String, Integer> allTopics = new ConcurrentHashMap<>();
//	protected ConsumerConnector connect;
//	protected Map<String, List<KafkaStream<byte[], byte[]>>> raws;
//	protected Map<String, Map<KafkaStream<byte[], byte[]>, V>> streams;
//
//	public KafkaInputBase0(String name, URISpec kafkaURI) throws ConfigException {
//		super(name);
//		init(kafkaURI, Texts.split(kafkaURI.getParameter("topic", ""), ","));
//	}
//
//	public KafkaInputBase0(String name, final String kafkaURI, String... topics) throws ConfigException, IOException {
//		super(name);
//		init(new URISpec(kafkaURI), Arrays.asList(topics));
//	}
//
//	private void init(URISpec uri, Collection<String> topics) throws ConfigException {
//		config = new KafkaInputConfig(name(), uri);
//		int kp = config.getPartitionParallelism();
//		logger.info("[" + name() + "] connecting with config [" + config.toString() + "].");
//		if (topics.isEmpty()) topics = config.topics();
//		Map<String, int[]> topicParts;
//		try (ZKConn zk = new ZKConn(config.getZookeeperConnect())) {
//			topicParts = zk.getTopicPartitions(topics.toArray(new String[topics.size()]));
//			for (String t : topics)
//				logger.debug(() -> "Lag of " + config.getGroupId() + "@" + t + ": " + zk.getLag(t, config.getGroupId()));
//		}
//		for (String t : topicParts.keySet()) {
//			if (kp <= 0) allTopics.put(t, 1);
//			else if (kp >= topicParts.get(t).length) allTopics.put(t, topicParts.get(t).length);
//			else allTopics.put(t, (int) Math.ceil(topicParts.get(t).length * 1.0 / kp));
//		}
//
//		logger.debug("[" + name() + "] parallelism of topics: " + allTopics.toString() + ".");
//		connect = kafka.consumer.Consumer.createJavaConsumerConnector(config.getConfig());
//		Map<String, List<KafkaStream<byte[], byte[]>>> temp = null;
//		do
//			try {
//				temp = connect.createMessageStreams(allTopics);
//			} catch (ConsumerRebalanceFailedException e) {
//				logger.warn("Kafka close and open too quickly, wait 10 seconds and retry");
//				if (!Concurrents.waitSleep(1000 * 10)) throw e;
//				temp = null;
//			}
//		while (temp == null);
//		raws = temp;
//		logger.debug("[" + name() + "] connected.");
//		streams = new HashMap<>();
//
//	}
//
//	protected abstract KafkaMessage fetch(KafkaStream<byte[], byte[]> stream, V lock, Consumer<KafkaMessage> result);
//
//	@Override
//	protected final void read(String topic, List<KafkaMessage> batch) {
//		for (Entry<KafkaStream<byte[], byte[]>, V> s : streams.get(topic).entrySet())
//			fetch(s.getKey(), s.getValue(), e -> batch.add(e));
//	}
//
//	@Override
//	public Stream<KafkaMessage> dequeue(long batchSize, Iterable<String> keys) {
//		try {
//			return super.dequeue(batchSize, keys);
//		} finally {
//			connect.commitOffsets(true);
//		}
//	}
//
//	@Override
//	public KafkaMessage dequeue(String topic) {
//		for (Entry<KafkaStream<byte[], byte[]>, V> s : streams.get(topic).entrySet()) {
//			KafkaMessage e = fetch(s.getKey(), s.getValue(), v -> connect.commitOffsets(false));
//			if (null != e) return e;
//		}
//		return null;
//	}
//
//	@Override
//	public Set<String> keys() {
//		return streams.keySet();
//	}
//
//	@Override
//	public void close() {
//		super.close(this::closeKafka);
//	}
//
//	private void closeKafka() {
//		for (Map<KafkaStream<byte[], byte[]>, V> tm : streams.values())
//			for (V f : tm.values())
//				if (f instanceof AutoCloseable) try {
//					((AutoCloseable) f).close();
//				} catch (Exception e) {
//					logger.error("[" + name() + "] internal fetcher close failure", e);
//				}
//		connect.commitOffsets(true);
//		connect.shutdown();
//	}
//}
