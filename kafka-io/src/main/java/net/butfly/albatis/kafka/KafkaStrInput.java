package net.butfly.albatis.kafka;

import static net.butfly.albacore.paral.Sdream.of;

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

import com.hzcominfo.albatis.nosql.Connection;

import kafka.common.ConsumerRebalanceFailedException;
import kafka.common.MessageStreamsExistException;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.paral.Task;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.kafka.config.KafkaInputConfig;

public class KafkaStrInput extends net.butfly.albacore.base.Namedly implements Input<Rmap> {
	private static final long serialVersionUID = 998704625489437241L;
	private final KafkaInputConfig config;
	private final Map<String, Integer> allTopics = Maps.of();
	private final ConsumerConnector connect;
	private final BlockingQueue<ConsumerIterator<String, String>> consumers;
	// for debug
	private final AtomicLong skip;

	private final Function<String, List<Map<String, Object>>> strDecoders;

	public KafkaStrInput(String name, String kafkaURI, String... topics) throws ConfigException,
			IOException {
		this(name, new URISpec(kafkaURI), topics);
	}

	public KafkaStrInput(String name, URISpec kafkaURI) throws ConfigException, IOException {
		this(name, kafkaURI, topic(kafkaURI).toArray(new String[0]));
	}

	private static Collection<String> topic(URISpec kafkaURI) {
		String topics = kafkaURI.getParameter("topic", kafkaURI.getFile());
		if (null == topics) throw new RuntimeException("Kafka topic not defined, as File segment of uri or [topic=TOPIC1,TOPIC2,...]");
		return Texts.split(topics, ",");
	}

	public KafkaStrInput(String name, URISpec uri, String... topics) throws ConfigException,
			IOException {
		super(name);
		this.strDecoders = Connection.strUriders(uri);
		config = new KafkaInputConfig(name(), uri);
		skip = new AtomicLong(Long.parseLong(uri.getParameter("skip", "0")));
		if (skip.get() > 0) logger().error("[" + name() + "] skip [" + skip.get()
				+ "] for testing, the skip is estimated, especially in multiple topic subscribing.");
		int configTopicParallinism = Props.propI(KafkaStrInput.class, "topic.paral", config.getDefaultPartitionParallelism());
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
		Map<String, List<KafkaStream<String, String>>> temp = null;
		ConsumerConnector c = null;
		do
			try {
				c = kafka.consumer.Consumer.createJavaConsumerConnector(config.getConfig());
				temp = c.createMessageStreams(allTopics, new StringDecoder(null), new StringDecoder(null));
			} catch (ConsumerRebalanceFailedException | MessageStreamsExistException e) {
				logger().warn("[" + name() + "] reopen too quickly, wait 10 seconds and retry");
				if (c != null) c.shutdown();
				temp = null;
				if (!Task.waitSleep(1000 * 10)) throw e;
			}
		while (temp == null);
		connect = c;
		logger().info("[" + name() + "] connected.");
		List<ConsumerIterator<String, String>> l = Colls.list();
		for (Entry<String, List<KafkaStream<String, String>>> e : temp.entrySet())
			for (KafkaStream<String, String> ks : e.getValue())
				l.add(ks.iterator());
		consumers = new LinkedBlockingQueue<>(l);
		closing(this::closeKafka);
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).<MessageAndMetadata<String, String>> sizing(km -> (long) km.rawMessage$1().payloadSize()) //
				.<MessageAndMetadata<String, String>> sampling(km -> new String(km.key())).detailing(Exeter.of()::toString);
	}

	@Override
	public void dequeue(Consumer<Sdream<Rmap>> using) {
		ConsumerIterator<String, String> it;
		MessageAndMetadata<String, String> m;
		while (opened())
			if (null != (it = consumers.poll())) {
				try {
					if (null != (m = it.next())) {
						List<Rmap> ms = Kafkas.strMessages((MessageAndMetadata<String, String>) s().stats(m), strDecoders);
						using.accept(of(ms));
						return;
					}
				} catch (ConsumerTimeoutException ex) {
					return;
				} catch (NoSuchElementException ex) {
					return;
				} catch (Exception ex) {
					logger().warn("Unprocessed kafka error [" + ex.getClass().toString() + ": " + ex.getMessage()
							+ "], ignore and continue.", ex);
					return;
				} finally {
					consumers.offer(it);
				}
			}
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
