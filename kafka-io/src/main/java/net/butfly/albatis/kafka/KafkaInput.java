package net.butfly.albatis.kafka;

import static net.butfly.albatis.ddl.TableDesc.dummy;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import kafka.common.ConsumerRebalanceFailedException;
import kafka.common.MessageStreamsExistException;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Task;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.kafka.config.KafkaInputConfig;

public class KafkaInput<T> extends Namedly implements OddInput<Rmap> {
	private static final long serialVersionUID = 998704625489437241L;
	private final KafkaInputConfig config;
	private final Map<String, Integer> allTopics = Maps.of();
	private ConsumerConnector connect;
	private final BlockingQueue<ConsumerIterator<T, T>> consumers;
	private final Function<T, String> keying;
	// for debug
	private final AtomicLong skip;

	public KafkaInput(String name, String kafkaURI, Class<T> nativeClass, TableDesc... topics) throws ConfigException, IOException {
		this(name, new URISpec(kafkaURI), nativeClass, topics);
	}

	public KafkaInput(String name, URISpec kafkaURI, Class<T> nativeClass) throws ConfigException, IOException {
		this(name, kafkaURI, nativeClass, dummy(topic(kafkaURI)));
	}

	protected static String[] topic(URISpec kafkaURI) {
		String topics = kafkaURI.getParameter("topic", kafkaURI.getFile());
		if (null == topics) throw new RuntimeException("Kafka topic not defined, as File segment of uri or [topic=TOPIC1,TOPIC2,...]");
		return Texts.split(topics, ",").toArray(new String[0]);
	}

	@SuppressWarnings("unchecked")
	public KafkaInput(String name, URISpec uri, Class<T> nativeClass, TableDesc... topics) throws ConfigException, IOException {
		super(name);
		config = new KafkaInputConfig(name(), uri);
		skip = new AtomicLong(Long.parseLong(uri.getParameter("skip", "0")));
		if (skip.get() > 0) logger().error("[" + name() + "] skip [" + skip.get()
				+ "] for testing, the skip is estimated, especially in multiple topic subscribing.");
		int configTopicParallinism = Props.propI(KafkaInput.class, "topic.paral", config.getDefaultPartitionParallelism());
		if (configTopicParallinism > 0) //
			logger().debug("[" + name() + "] default topic parallelism [" + configTopicParallinism + "]");
		if (topics == null || topics.length == 0) topics = dummy(config.topics());
		Set<String> ts = new HashSet<>(Colls.list(t -> t.name, topics));
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

		Decoder<T> d;
		if (String.class.isAssignableFrom(nativeClass)) {
			keying = s -> (String) s;
			d = (Decoder<T>) new DefaultDecoder(null);
		} else if (byte[].class.isAssignableFrom(nativeClass)) {
			keying = b -> new String((byte[]) b);
			d = (Decoder<T>) new StringDecoder(null);
		} else throw new IllegalArgumentException();
		consumers = new LinkedBlockingQueue<>(createConsumers(d));
		closing(this::closeKafka);
	}

	private List<ConsumerIterator<T, T>> createConsumers(Decoder<T> d) throws ConfigException {
		Map<String, List<KafkaStream<T, T>>> temp = null;
		ConsumerConnector c = null;
		do
			try {
				c = kafka.consumer.Consumer.createJavaConsumerConnector(config.getConfig());
				temp = c.createMessageStreams(allTopics, d, d);
			} catch (ConsumerRebalanceFailedException | MessageStreamsExistException e) {
				logger().warn("[" + name() + "] reopen too quickly, wait 10 seconds and retry");
				if (c != null) c.shutdown();
				temp = null;
				if (!Task.waitSleep(1000 * 10)) throw e;
			}
		while (temp == null);
		connect = c;
		logger().info("[" + name() + "] connected.");
		List<ConsumerIterator<T, T>> l = Colls.list();
		for (Entry<String, List<KafkaStream<T, T>>> e : temp.entrySet())
			for (KafkaStream<T, T> ks : e.getValue())
				l.add(ks.iterator());
		return l;
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).<MessageAndMetadata<byte[], byte[]>> sizing(km -> (long) km.rawMessage$1().payloadSize()) //
				.<MessageAndMetadata<byte[], byte[]>> sampling(km -> new String(km.key())).detailing(Exeter.of()::toString);
	}

	@Override
	public Rmap dequeue() {
		ConsumerIterator<T, T> it;
		MessageAndMetadata<T, T> m;
		while (opened())
			if (null != (it = consumers.poll())) {
				try {
					if (null != (m = it.next())) {
						MessageAndMetadata<T, T> km = (MessageAndMetadata<T, T>) s().stats(m);
						String k = keying.apply(km.key());
						return new Rmap(km.topic(), k, k, km.message());
					}
				} catch (ConsumerTimeoutException ex) {
					return null;
				} catch (NoSuchElementException ex) {
					return null;
				} catch (Exception ex) {
					logger().warn("Unprocessed kafka error [" + ex.getClass().toString() + ": " + ex.getMessage()
							+ "], ignore and continue.", ex);
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
