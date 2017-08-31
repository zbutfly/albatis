package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

import kafka.common.ConsumerRebalanceFailedException;
import kafka.common.MessageStreamsExistException;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Streams;
import net.butfly.albacore.utils.parallel.Concurrents;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.InputOddImpl;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.kafka.config.KafkaInputConfig;

public final class KafkaInput extends Namedly implements Input<Message> {
	private static final long POOL_LOCK_WAITING = -1;
	private final KafkaInputConfig config;
	private final Map<String, Integer> allTopics = new ConcurrentHashMap<>();
	private final ConsumerConnector connect;
	private final BlockingQueue<ConsumerOutput> consumers;
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
		consumers = new LinkedBlockingQueue<>();
		int i = 0;
		for (String topic : temp.keySet())
			for (KafkaStream<byte[], byte[]> s : temp.get(topic))
				consumers.offer(new ConsumerOutput(topic, s, i++));
		closing(this::closeKafka);
		open();
	}

	@Override
	public long dequeue(Function<Stream<Message>, Long> using, int batchSize) {
		return using.apply(Streams.of(() -> {
			ConsumerOutput c = null;
			if (null == (c = consumers.poll())) return null;
			try {
				return c.dequeue();
			} finally {
				consumers.offer(c);
			}
		}, batchSize, () -> opened()));
	}

	@Override
	public void open() {
		for (ConsumerOutput f : consumers)
			f.open();
		Input.super.open();
	}

	private void closeKafka() {
		for (ConsumerOutput f : consumers)
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

	class ConsumerOutput extends InputOddImpl<Message> {
		public final String topic;
		public final ConsumerIterator<byte[], byte[]> it;
		public final KafkaStream<byte[], byte[]> stream;

		public ConsumerOutput(String topic, KafkaStream<byte[], byte[]> stream, int i) {
			super(KafkaInput.this.name() + "#Consumer@" + topic + "#" + i);
			this.topic = topic;
			this.stream = stream;
			it = stream.iterator();
		}

		public boolean existMore() {
			while (opened())
				try {
					return it.hasNext();
				} catch (ConsumerTimeoutException ex) {
					Concurrents.waitSleep(POOL_LOCK_WAITING);
				}
			return opened();
		}

		@Override
		protected Message dequeue() {
			Message m = null;
			while (opened() && existMore()) {
				try {
					return Kafkas.message(it.next(), decoder);
				} catch (Exception e) {
					logger().warn("[Fetcher: " + name() + "] reading failed and ignore...", e);
				}
			}
			return m;
		}
	}
}
