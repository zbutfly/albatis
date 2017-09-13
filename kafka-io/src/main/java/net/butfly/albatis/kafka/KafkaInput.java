package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
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
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Task;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.parallel.Concurrents;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.kafka.config.KafkaInputConfig;

public final class KafkaInput extends OddInput<Message> {
	private final KafkaInputConfig config;
	private final Map<String, Integer> allTopics = Maps.of();
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
		this(name, kafkaURI, decoder, topic(kafkaURI).toArray(new String[0]));
	}

	private static Collection<String> topic(URISpec kafkaURI) {
		String topics = kafkaURI.getParameter("topic", kafkaURI.getFile());
		if (null == topics) throw new RuntimeException("Kafka topic not defined, as File segment of uri or [topic=TOPIC1,TOPIC2,...]");
		return Texts.split(topics, ",");
	}

	public KafkaInput(String name, URISpec uri, Function<byte[], Map<String, Object>> decoder, String... topics) throws ConfigException,
			IOException {
		super(name);
		this.decoder = decoder;
		config = new KafkaInputConfig(name(), uri);
		skip = new AtomicLong(Long.parseLong(uri.getParameter("skip", "0")));
		if (skip.get() > 0) logger().error("[" + name() + "] skip [" + skip.get()
				+ "] for testing, the skip is estimated, especially in multiple topic subscribing.");
		int configTopicParallinism = Props.propI(KafkaInput.class, "topic.paral", config.getDefaultPartitionParallelism());
		if (configTopicParallinism > 0) //
			logger().debug("[" + name() + "] default topic parallelism [" + configTopicParallinism + "]");
		Set<String> ts = topics == null || topics.length == 0 ? new HashSet<>(config.topics()) : new HashSet<>(Arrays.asList(topics));
		Map<String, Integer> topicPartitions = config.getTopicPartitions(ts);

		for (String t : ts) {
			int parts = topicPartitions.getOrDefault(t, 1);
			int p = configTopicParallinism <= 0 ? parts : configTopicParallinism;
			allTopics.put(t, p);
			if (p > parts) logger().warn("[" + name() + "] topic [" + t + "] define parallelism: [" + p + "] "
					+ "over existed partitions: [" + parts + "].");
			else logger().info("[" + name() + "] topic [" + t + "] consumers creating as parallelism [" + p + "]");
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
				if (!Task.waitSleep(1000 * 10)) throw e;
			}
		while (temp == null);
		connect = c;
		logger().info("[" + name() + "] connected.");
		consumers = new LinkedBlockingQueue<>(temp.entrySet().stream().flatMap(e -> e.getValue().stream().map(s -> s.iterator())).collect(
				Collectors.toList()));
		closing(this::closeKafka);
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
