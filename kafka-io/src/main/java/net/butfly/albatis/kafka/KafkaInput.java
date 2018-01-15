package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

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
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.kafka.config.KafkaInputConfig;

public class KafkaInput extends net.butfly.albacore.base.Namedly implements OddInput<Message> {
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
		int kp = Props.propI(KafkaInput.class, "topic.paral", -1);
		if (kp <= 0) kp = config.getPartitionParallelism();
		else logger().debug("[" + name() + "] override topic parallelism by setting [" + kp + "]");
		Set<String> ts = topics == null || topics.length == 0 ? new HashSet<>(config.topics()) : new HashSet<>(Arrays.asList(topics));
		Map<String, int[]> topicParts;
		try (ZKConn zk = new ZKConn(config.getZookeeperConnect())) {
			topicParts = zk.getTopicPartitions(topics);
			if (ts.isEmpty()) {
				ts = topicParts.keySet();
				logger().warn("[" + name() + "] not define topic, try to read all topic on kafka: " + ts.toString());
			}
			if (logger().isDebugEnabled()) for (String t : ts)
				logger().debug("[" + name() + "] lag of " + config.getGroupId() + "@" + t + ": " + zk.getLag(t, config.getGroupId()));
		}
		for (String t : ts) {
			int[] zk = topicParts.get(t);
			if (kp <= 0) kp = null != zk && zk.length > 0 ? topicParts.get(t).length : 1;
			if (null != zk && zk.length > 0 && kp > topicParts.get(t).length) //
				logger().warn("[" + name() + "] topic [" + t + "] define parallelism: [" + kp + "] " + //
						"over partitions: [" + topicParts.get(t).length + "].");
			allTopics.put(t, kp);
			logger().info("[" + name() + "] topic [" + t + "] consumers creating as parallelism [" + kp + "]");
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
		List<ConsumerIterator<byte[], byte[]>> l = Colls.list();
		for (Entry<String, List<KafkaStream<byte[], byte[]>>> e : temp.entrySet())
			for (KafkaStream<byte[], byte[]> ks : e.getValue())
				l.add(ks.iterator());
		consumers = new LinkedBlockingQueue<>(l);
		closing(this::closeKafka);
		open();
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).<MessageAndMetadata<byte[], byte[]>> sizing(km -> (long) km.rawMessage$1().payloadSize()) //
				.<MessageAndMetadata<byte[], byte[]>> sampling(km -> new String(km.key())).detailing(Exeter.of()::toString);
	}

	@Override
	public Message dequeue() {
		ConsumerIterator<byte[], byte[]> it;
		MessageAndMetadata<byte[], byte[]> m;
		while (opened())
			if (null != (it = consumers.poll())) {
				try {
					if (null != (m = it.next())) //
						return Kafkas.message((MessageAndMetadata<byte[], byte[]>) s().stats(m), decoder);
				} catch (ConsumerTimeoutException ex) {
					return null;
				} catch (NoSuchElementException ex) {
					return null;
				} catch (Exception ex) {
					logger().warn("Unprocessed kafka error [" + ex.getClass().toString() + ": " + ex.getMessage()
							+ "], ignore and continue.");
					return null;
				} finally {
					consumers.offer(it);
				}
			}
		return null;
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
