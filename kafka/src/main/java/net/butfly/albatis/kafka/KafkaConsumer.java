package net.butfly.albatis.kafka;

import java.io.File;
import java.io.IOException;
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
import net.butfly.albatis.impl.kafka.config.KafkaConsumerConfig;
import net.butfly.albatis.impl.kafka.config.KafkaTopicConfig;
import net.butfly.albatis.impl.kafka.mapper.KafkaMessage;
import scala.Tuple2;

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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List<KafkaMessage> readMap(String topic) {
		TypeToken<Map> t = TypeToken.of(Map.class);
		List<KafkaMessage> l = new ArrayList<>();

		for (Tuple2<String, byte[]> message : context.dequeue(topic)) {
			KafkaMessage km = new KafkaMessage();
			Map map = (Map) serder.der(message._2, t).get("value");
			km.setTopic(message._1);
			if (topicKeys.containsKey(topic)) km.setKey(map.get(topicKeys.get(topic)));
			km.setValue(map);
			l.add(km);
		}
		return l;
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
