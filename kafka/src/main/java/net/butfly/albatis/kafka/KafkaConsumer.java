package net.butfly.albatis.kafka;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

public class KafkaConsumer implements AutoCloseable {
	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

	private String name;
	private Map<String, String> topicKeys = new HashMap<>();
	private Map<String, Integer> topicConcurrencies = new HashMap<>();

	private ConsumerConnector connect;
	private Queue context;
	private ExecutorService threads;
	private BsonSerder serder;
	private boolean mixed;

	/**
	 * @param mixed:
	 *            是否混合模式
	 * @param maxPackageMix:
	 *            混合模式下缓冲区个数
	 * @param maxMessageNoMix:
	 *            缓冲消息个数
	 * @return
	 */
	public KafkaConsumer(String name, final KafkaConsumerConfig config, final KafkaTopicConfig[] topics, final boolean mixed,
			final int maxMixPackage, final int maxMessage) throws KafkaException {
		super();
		this.name = name;
		this.serder = new BsonSerder();
		int concurrency = parseTopics(topics);
		if (concurrency == 0) throw new KafkaException("Kafka configuration has no topic definition.");
		threads = Executors.newFixedThreadPool(concurrency);
		this.mixed = mixed;
		logger.info("[" + name + "] Consumer thread starting (max: " + concurrency + ")...");
		createConsumers(configure(config, mixed), topics);
	}

	@SuppressWarnings("serial")
	private static final TypeToken<Map<String, Object>> T_MAP = new TypeToken<Map<String, Object>>() {};

	@SuppressWarnings("unchecked")
	public List<KafkaMapWrapper> read(String... topic) {
		List<KafkaMapWrapper> l = new ArrayList<>();
		if (mixed) topic = new String[] { null };
		for (String t : topic) {
			List<Tuple2<String, byte[]>> all = context.dequeue(t);
			if (context.size(t) == 0) commit();
			for (Tuple2<String, byte[]> message : all) {
				Map<String, Object> meta = serder.der(message._2, T_MAP);
				Map<String, Object> value = (Map<String, Object>) meta.get("value");
				meta.remove("value");
				l.add(new KafkaMapWrapper(t, topicKeys.containsKey(topic) ? value.get(topicKeys.get(topic)) : null, meta, value));
			}
		}
		return l;
	}

	public <E> List<KafkaObjectWrapper<E>> read(Class<E> cl, String... topic) {
		@SuppressWarnings("serial")
		TypeToken<KafkaObjectWrapper<E>> t = new TypeToken<KafkaObjectWrapper<E>>() {};
		List<KafkaObjectWrapper<E>> l = new ArrayList<>();

		if (mixed) topic = new String[] { null };
		for (String top : topic) {
			List<Tuple2<String, byte[]>> all = context.dequeue(top);
			if (context.size(top) == 0) commit();
			for (Tuple2<String, byte[]> message : all) {
				KafkaObjectWrapper<E> w = serder.der(message._2, t);
				w.setTopic(top);
				if (topicKeys.containsKey(topic)) w.setKey(Reflections.get(w.getValue(), topicKeys.get(topic)));
				l.add(w);
			}
		}
		return l;
	}

	public void commit() {
		connect.commitOffsets();
	}

	public void close() {
		logger.info("[" + name + "] All threads closing...");
		threads.shutdown();
		connect.shutdown();
		logger.info("[" + name + "] Buffer clearing...");
		context.close();
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

	private ConsumerConfig configure(KafkaConsumerConfig config, boolean mixed) throws KafkaException {
		context = mixed ? new QueueMixed(curr(config.toString()), 1000) : new QueueTopic(curr(config.toString()), 1000);
		return config.getConfig();
	}

	private String curr(String zookeeperConnect) throws KafkaException {
		try {
			File f = new File("./" + zookeeperConnect.replaceAll("[:/\\,]", "-"));
			f.mkdirs();
			return f.getCanonicalPath();
		} catch (IOException e) {
			throw new KafkaException(e);
		}
	}

	private void createConsumers(ConsumerConfig config, KafkaTopicConfig... topics) {
		int c = 0;
		try {
			connect = Consumer.createJavaConsumerConnector(config);
			for (Map.Entry<String, List<KafkaStream<byte[], byte[]>>> messageStreamsOfTopic : connect.createMessageStreams(
					topicConcurrencies).entrySet()) {
				List<KafkaStream<byte[], byte[]>> streams = messageStreamsOfTopic.getValue();
				if (streams.isEmpty()) continue;
				for (final KafkaStream<byte[], byte[]> stream : streams) {
					threads.submit(new ConsumerThread(context, stream));
					c++;
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Kafka connecting failure.", e);
		}
		logger.info("[" + name + "] Consumer thread started (current: " + c + ").");
	}
}
