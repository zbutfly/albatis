package net.butfly.albatis.kafka;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.TypeToken;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albatis.impl.kafka.config.KafkaConsumerConfig;
import net.butfly.albatis.impl.kafka.config.KafkaTopicConfig;
import scala.Tuple2;
import scala.Tuple3;

public class KafkaConsumer implements AutoCloseable {
	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

	boolean inited = false;
	private String name;
	private Map<String, String> topicKeys = new HashMap<>();
	private Map<String, Integer> topicConcurrencies = new HashMap<>();

	private ConsumerConnector connect;
	private KafkaQueue context;
	private ExecutorService consumers;
	private BsonSerder serder;

	public KafkaConsumer() {
		this("COMINFO_KAFKA_CONSUMER");
	}

	public KafkaConsumer(String name) {
		super();
		this.name = name;
		this.serder = new BsonSerder();
	}

	@SuppressWarnings("serial")
	private static final TypeToken<Map<String, Object>> T_MAP = new TypeToken<Map<String, Object>>() {};

	@SuppressWarnings("unchecked")
	public List<Tuple3<String, Object, Map<String, Object>>> read(String topic) {
		List<Tuple3<String, Object, Map<String, Object>>> l = new ArrayList<>();
		for (Tuple2<String, byte[]> message : context.dequeue(topic)) {
			Map<String, Object> map = (Map<String, Object>) serder.der(message._2, T_MAP).get("value");
			Object key = topicKeys.containsKey(topic) ? map.get(topicKeys.get(topic)) : null;
			l.add(new Tuple3<String, Object, Map<String, Object>>(message._1, key, map));
		}
		return l;
	}

	public static class KafkaWrapper<E> implements Serializable {
		private static final long serialVersionUID = -1808631723718939410L;

		public E getValue() {
			return value;
		}

		public void setValue(E value) {
			this.value = value;
		}

		private E value;
	}

	public <E> List<Tuple3<String, Object, E>> read(String topic, Class<E> cl) {
		@SuppressWarnings("serial")
		TypeToken<KafkaWrapper<E>> t = new TypeToken<KafkaWrapper<E>>() {};
		List<Tuple3<String, Object, E>> l = new ArrayList<>();

		for (Tuple2<String, byte[]> message : context.dequeue(topic)) {
			KafkaWrapper<E> e = serder.der(message._2, t);
			Object key = topicKeys.containsKey(topic) ? Reflections.get(e.value, topicKeys.get(topic)) : null;
			l.add(new Tuple3<String, Object, E>(message._1, key, e.value));
		}
		return l;
	}

	/**
	 * @param config
	 * @param topics
	 * @param isMix:
	 *            是否混合模式
	 * @param maxPackageMix:
	 *            混合模式下缓冲区个数
	 * @param maxMessageNoMix:
	 *            缓冲消息个数
	 */
	public void initialize(final KafkaConsumerConfig config, final KafkaTopicConfig[] topics, final boolean isMix, final int maxMixPackage,
			final int maxMessage) {
		if (inited) throw new RuntimeException("Kafka consumer has already been initialized.");
		// TODO

		int threads = parseTopics(topics);
		if (threads == 0) throw new RuntimeException("Kafka configuration has no topic definition.");
		consumers = Executors.newFixedThreadPool(threads);
		createConsumers(threads, configure(config), topics);
		inited = true;
	}

	public void commit() {
		connect.commitOffsets();
	}

	public void close() {
		logger.info("[" + name + "] All consumers closing...");
		consumers.shutdown();
		connect.shutdown();
		logger.info("[" + name + "] Buffer clearing...");
		context.close();
		inited = false;
	}

	private int parseTopics(KafkaTopicConfig[] topics) {
		int count = 0;
		for (KafkaTopicConfig topic : topics) {
			count += topic.getStreamNum();
			topicConcurrencies.put(topic.getTopic(), topic.getStreamNum());
			topicKeys.put(topic.getTopic(), topic.getKey());
		}
		return count;
	}

	private ConsumerConfig configure(KafkaConsumerConfig config) {
		context = new KafkaQueue(curr(config.getZookeeperConnect()), 1000);
		if (config.getZookeeperConnect() == null || config.getGroupId() == null) throw new RuntimeException(
				"Kafka configuration has no zookeeper and group definition.");
		Properties props = new Properties();
		props.put("zookeeper.connect", config.getZookeeperConnect());
		props.put("zookeeper.connection.timeout.ms", "" + config.getZookeeperConnectionTimeoutMs());
		props.put("zookeeper.sync.time.ms", "" + config.getZookeeperSyncTimeMs());
		props.put("group.id", config.getGroupId());
		props.put("auto.commit.enable", "" + config.isAutoCommitEnable());
		props.put("auto.commit.interval.ms", "" + config.getAutoCommitIntervalMs());
		props.put("auto.offset.reset", config.getAutoOffsetReset());
		props.put("session.timeout.ms", "" + config.getSessionTimeoutMs());
		props.put("partition.assignment.strategy", config.getPartitionAssignmentStrategy());
		props.put("socket.receive.buffer.bytes", "" + config.getSocketReceiveBufferBytes());
		props.put("fetch.message.max.bytes", "" + config.getFetchMessageMaxBytes());
		return new ConsumerConfig(props);
	}

	private String curr(String zookeeperConnect) {
		try {
			File f = new File("./" + zookeeperConnect.replaceAll("[:/\\,]", "-"));
			f.mkdirs();
			return f.getCanonicalPath();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void createConsumers(int threads, ConsumerConfig config, KafkaTopicConfig... topics) {
		logger.info("[" + name + "] Consumer thread starting (max: " + threads + ")...");
		int c = 0;
		try {
			connect = Consumer.createJavaConsumerConnector(config);
			for (Map.Entry<String, List<KafkaStream<byte[], byte[]>>> messageStreamsOfTopic : connect.createMessageStreams(
					topicConcurrencies).entrySet()) {
				List<KafkaStream<byte[], byte[]>> streams = messageStreamsOfTopic.getValue();
				if (streams.isEmpty()) continue;
				for (final KafkaStream<byte[], byte[]> stream : streams) {
					ConsumerThread ct = new ConsumerThread(context);
					ct.setStream(stream);
					consumers.submit(ct);
					c++;
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Kafka connecting failure.", e);
		}
		logger.info("[" + name + "] Consumer thread started (current: " + c + ").");
	}
}
