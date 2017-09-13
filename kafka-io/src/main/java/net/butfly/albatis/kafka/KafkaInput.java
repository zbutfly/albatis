package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import kafka.common.ConsumerRebalanceFailedException;
import kafka.common.MessageStreamsExistException;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.parallel.Concurrents;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.kafka.config.KafkaInputConfig;

public final class KafkaInput extends OddInput<Message> {
	private final KafkaInputConfig config;
	private final Map<String, Integer> allTopics = new ConcurrentHashMap<>();
	private final ConsumerConnector connect;
	private final BlockingQueue<ConsumerIterator<byte[], byte[]>> consumers;
	// for debug
	private final AtomicLong skip;

	private final Function<byte[], Map<String, Object>> decoder;

	public KafkaInput(String name, String kafkaURI, Function<byte[], Map<String, Object>> decoder, String... topics) throws ConfigException,
			IOException {
		this(name, new URISpec(kafkaURI), decoder, topics);
	}

	public KafkaInput(String name, URISpec kafkaURI, Function<byte[], Map<String, Object>> decoder) throws ConfigException, IOException {
		this(name, kafkaURI, decoder, Texts.split(kafkaURI.getParameter("topic", ""), ",").toArray(new String[0]));
	}

	public KafkaInput(String name, URISpec uri, Function<byte[], Map<String, Object>> decoder, String... topics) throws ConfigException,
			IOException {
		super(name);
		this.decoder = decoder;
		config = new KafkaInputConfig(name(), uri);
		skip = new AtomicLong(Long.parseLong(uri.getParameter("skip", "0")));
		if (skip.get() > 0) logger().error("[" + name() + "] skip [" + skip.get()
				+ "] for testing, the skip is estimated, especially in multiple topic subscribing.");
		int kp = config.getPartitionParallelism();
		if (topics == null || topics.length == 0) topics = config.topics().toArray(new String[0]);
		Map<String, int[]> topicParts;
		try (ZKConn zk = new ZKConn(config.getZookeeperConnect())) {
			topicParts = zk.getTopicPartitions(topics);
			if (logger().isTraceEnabled()) for (String t : topics)
				logger().debug(() -> "[" + name() + "] lag of " + config.getGroupId() + "@" + t + ": " + zk.getLag(t, config.getGroupId()));
		}
		for (String t : topicParts.keySet()) {
			if (kp <= 0) allTopics.put(t, 1);
			else if (kp >= topicParts.get(t).length) allTopics.put(t, topicParts.get(t).length);
			else allTopics.put(t, (int) Math.ceil(topicParts.get(t).length * 1.0 / kp));
		}
		logger().trace("[" + name() + "] parallelism of topics: " + allTopics.toString() + ".");
		// connect
		Map<String, List<KafkaStream<byte[], byte[]>>> temp = null;
		ConsumerConnector c = null;
		do
			try {
				c = kafka.consumer.Consumer.createJavaConsumerConnector(config.getConfig());
				temp = c.createMessageStreams(allTopics);
			} catch (ConsumerRebalanceFailedException | MessageStreamsExistException e) {
				logger().warn("[" + name() + "] reopen too quickly, wait 10 seconds and retry");
				if (c != null) c.shutdown();
				temp = null;
				if (!Concurrents.waitSleep(1000 * 10)) throw e;
			}
		while (temp == null);
		connect = c;
		logger().info("[" + name() + "] connected.");
		consumers = new LinkedBlockingQueue<>(temp.entrySet().stream().flatMap(e -> e.getValue().stream().map(s -> s.iterator())).collect(
				Collectors.toList()));
		closing(this::closeKafka);
		open();
	}

	@Override
	protected Message dequeue() {
		ConsumerIterator<byte[], byte[]> it;
		MessageAndMetadata<byte[], byte[]> m;
		if (null == (it = consumers.poll())) return null;
		try {
			while (true)
				try {
					if (null != (m = it.next())) break;
				} catch (ConsumerTimeoutException ex) {
					continue;
				} catch (NoSuchElementException ex) {
					return null;
				} catch (Exception ex) {
					logger().warn("Unprocessed kafka error", ex);
					return null;
				}
		} finally {
			consumers.offer(it);
		}
		return Kafkas.message(m, decoder);
	}

	private void closeKafka() {
		try {
			connect.commitOffsets(true);
		} catch (Exception e) {
			logger().error("[" + name() + "] commit fail", e);
		}
		try {
			connect.shutdown();
		} catch (Exception e) {
			logger().error("[" + name() + "] shutdown fail", e);
		}

	}
}
