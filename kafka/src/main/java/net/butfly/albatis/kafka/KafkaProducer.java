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
import net.butfly.albacore.utils.Reflections;
import net.butfly.albatis.impl.kafka.config.KafkaConsumerConfig;
import net.butfly.albatis.impl.kafka.config.KafkaTopicConfig;
import scala.Tuple2;

public class KafkaProducer implements AutoCloseable {
	private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

	boolean inited = false;
	private String name;
	private Map<String, String> topicKeys = new HashMap<>();
	private Map<String, Integer> topicConcurrencies = new HashMap<>();

	private ConsumerConnector connect;
	private Queue context;
	private ExecutorService consumers;
	private BsonSerder serder;
	private boolean mixed;

	public KafkaProducer() {
		this("COMINFO_KAFKA_CONSUMER");
	}

	public KafkaProducer(String name) {
		super();
		this.name = name;
		this.serder = new BsonSerder();
	}

	@SuppressWarnings("serial")
	private static final TypeToken<Map<String, Object>> T_MAP = new TypeToken<Map<String, Object>>() {};

	@SuppressWarnings("unchecked")
	public List<KafkaMapWrapper> read(String... topic) {
		List<KafkaMapWrapper> l = new ArrayList<>();
		if (mixed) topic = new String[] { null };
		for (String t : topic)
			for (Tuple2<String, byte[]> message : context.dequeue(t)) {
				Map<String, Object> meta = serder.der(message._2, T_MAP);
				Map<String, Object> value = (Map<String, Object>) meta.get("value");
				meta.remove("value");
				l.add(new KafkaMapWrapper(t, topicKeys.containsKey(topic) ? value.get(topicKeys.get(topic)) : null, meta, value));
			}
		return l;
	}

	public <E> List<KafkaObjectWrapper<E>> read(Class<E> cl, String... topic) {
		@SuppressWarnings("serial")
		TypeToken<KafkaObjectWrapper<E>> t = new TypeToken<KafkaObjectWrapper<E>>() {};
		List<KafkaObjectWrapper<E>> l = new ArrayList<>();

		if (mixed) topic = new String[] { null };
		for (String top : topic)
			for (Tuple2<String, byte[]> message : context.dequeue(top)) {
				KafkaObjectWrapper<E> w = serder.der(message._2, t);
				w.setTopic(top);
				if (topicKeys.containsKey(topic)) w.setKey(Reflections.get(w.getValue(), topicKeys.get(topic)));
				l.add(w);
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
	 * @return
	 */
	public KafkaProducer initialize(final KafkaConsumerConfig config, final KafkaTopicConfig[] topics, final boolean mixed,
			final int maxMixPackage, final int maxMessage) {
		if (inited) throw new RuntimeException("Kafka consumer has already been initialized.");
		int threads = parseTopics(topics);
		if (threads == 0) throw new RuntimeException("Kafka configuration has no topic definition.");
		consumers = Executors.newFixedThreadPool(threads);
		this.mixed = mixed;
		createConsumers(threads, configure(config, mixed), topics);
		inited = true;
		return this;
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

	private ConsumerConfig configure(KafkaConsumerConfig config, boolean mixed) {
		context = mixed ? new QueueMixed(curr(config.getZookeeperConnect()), 1000)
				: new QueueTopic(curr(config.getZookeeperConnect()), 1000);
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
