package net.butfly.albatis.kafka;

import static net.butfly.albacore.io.utils.Streams.of;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import kafka.common.ConsumerRebalanceFailedException;
import kafka.common.MessageStreamsExistException;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.InputImpl;
import net.butfly.albacore.io.utils.Streams;
import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.parallel.Concurrents;
import net.butfly.albatis.kafka.config.KafkaInputConfig;

public final class KafkaInput1 extends InputImpl<KafkaMessage> {
	private final KafkaInputConfig config;
	private final Map<String, Integer> allTopics = new ConcurrentHashMap<>();
	private final ConsumerConnector connect;
	private final Map<String, List<KafkaStream<byte[], byte[]>>> streamMap;
	private final List<KafkaStream<byte[], byte[]>> streams;

	public KafkaInput1(String name, String kafkaURI, String... topics) throws ConfigException, IOException {
		this(name, new URISpec(kafkaURI), topics);
	}

	public KafkaInput1(String name, URISpec kafkaURI) throws ConfigException, IOException {
		this(name, kafkaURI, Texts.split(kafkaURI.getParameter("topic", ""), ",").toArray(new String[0]));
	}

	public KafkaInput1(String name, URISpec uri, String... topics) throws ConfigException, IOException {
		super(name);
		config = new KafkaInputConfig(name(), uri);
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
		Pair<ConsumerConnector, Map<String, List<KafkaStream<byte[], byte[]>>>> p = connect();
		connect = p.v1();
		streamMap = p.v2();
		closing(this::closeKafka);
		streams = new ArrayList<>(of(streamMap.values()).flatMap(Streams::of).collect(Collectors.toSet()));
		open();
	}

	// reconnect
	private Pair<ConsumerConnector, Map<String, List<KafkaStream<byte[], byte[]>>>> connect() throws ConfigException {
		ConsumerConnector c = null;
		Map<String, List<KafkaStream<byte[], byte[]>>> map = null;
		do
			try {
				c = kafka.consumer.Consumer.createJavaConsumerConnector(config.getConfig());
				map = c.createMessageStreams(allTopics);
			} catch (ConsumerRebalanceFailedException | MessageStreamsExistException e) {
				logger().warn("[" + name() + "] reopen too quickly, wait 10 seconds and retry");
				if (c != null) c.shutdown();
				if (!Concurrents.waitSleep(1000 * 10)) throw e;
			}
		while (c == null);
		logger().info("[" + name() + "] connected.");
		return new Pair<>(c, map);
	}

	private void closeKafka() {
		for (List<KafkaStream<byte[], byte[]>> f : streamMap.values())
			if (f instanceof AutoCloseable) try {
				((AutoCloseable) f).close();
			} catch (Exception e) {
				logger().error("[" + name() + "] internal fetcher close failure", e);
			}
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

	private AtomicInteger curr = new AtomicInteger(0);

	private ConsumerIterator<byte[], byte[]> it() {
		return streams.get(curr.getAndUpdate(i -> (i + 1) % streams.size())).iterator();
	}

	private boolean hasNext(ConsumerIterator<byte[], byte[]> it) {
		while (opened())
			try {
				return it.hasNext();
			} catch (ConsumerTimeoutException ex) {
				Concurrents.waitSleep();
			}
		return opened();
	}

	@Override
	protected KafkaMessage dequeue() {
		while (opened())
			try {
				while (opened()) {
					ConsumerIterator<byte[], byte[]> it = it();
					if (!hasNext(it)) continue;
					MessageAndMetadata<byte[], byte[]> m = it.next();
					if (null == m) continue;
					return new KafkaMessage(m);
				}
			} catch (Exception e) {
				logger().warn("[Fetcher: " + name() + "] reading failed and ignore...", e);
			}
		return null;
	}
}
