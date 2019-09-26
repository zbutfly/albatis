package net.butfly.albatis.kafka;

import static net.butfly.albatis.ddl.TableDesc.dummy;
import static net.butfly.albatis.io.IOProps.propI;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import kafka.common.ConsumerRebalanceFailedException;
import kafka.common.MessageStreamsExistException;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Task;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.kafka.config.KafkaInputConfig;

public class KafkaInput extends Namedly implements KafkaIn {
	private static final long serialVersionUID = 998704625489437241L;
	private final KafkaInputConfig config;
	private final Map<String, Integer> allTopics = Maps.of();
	private ConsumerConnector connect;
	@SuppressWarnings("rawtypes")
	private final BlockingQueue<ConsumerIterator> consumers;
	// private final Function<T, String> keying;
	// for debug
	private final AtomicLong skip;

	public KafkaInput(String name, String kafkaURI, TableDesc... topics) throws ConfigException, IOException {
		this(name, new URISpec(kafkaURI), topics);
	}

	public KafkaInput(String name, URISpec kafkaURI) throws ConfigException, IOException {
		this(name, kafkaURI, dummy(topic(kafkaURI)).toArray(new TableDesc[0]));
	}

	protected static String[] topic(URISpec kafkaURI) {
		String topics = kafkaURI.getParameter("topic", kafkaURI.getFile());
		if (null == topics) throw new RuntimeException("Kafka topic not defined, as File segment of uri or [topic=TOPIC1,TOPIC2,...]");
		return Texts.split(topics, ",").toArray(new String[0]);
	}

	public KafkaInput(String name, URISpec uri, TableDesc... topics) throws ConfigException, IOException {
		super(name);
		config = new KafkaInputConfig(name(), uri);
		skip = new AtomicLong(Long.parseLong(uri.getParameter("skip", "0")));
		if (skip.get() > 0) logger().error("[" + name() + "] skip [" + skip.get()
				+ "] for testing, the skip is estimated, especially in multiple topic subscribing.");
		int configTopicParallinism = propI(KafkaInput.class, "topic.paral", config.getDefaultPartitionParallelism());
		if (configTopicParallinism > 0) //
			logger().debug("[" + name() + "] default topic parallelism [" + configTopicParallinism + "]");
		if (topics == null || topics.length == 0) topics = dummy(config.topics()).toArray(new TableDesc[0]);
		Set<String> ts = new HashSet<>(Colls.list(t -> t.qualifier.name, topics));
		Map<String, Integer> topicPartitions = config.getTopicPartitions(ts.toArray(new String[ts.size()]));

		for (String t : ts) {
			int parts = topicPartitions.getOrDefault(t, 1);
			int p = configTopicParallinism <= 0 ? parts : configTopicParallinism;
			allTopics.put(t, p);
			if (p > parts) //
				logger().warn("[" + name() + "] topic [" + t + "] define parallelism: [" + p + "] over existed partitions: [" + parts + "].");
			else logger().info("[" + name() + "] topic [" + t + "] consumers creating as parallelism [" + p + "]");
		}
		logger().trace("[" + name() + "] parallelism of topics: " + allTopics.toString() + ".");

		// if (String.class.isAssignableFrom(nativeClass)) {
		// keying = s -> (String) s;
		// consumers = new LinkedBlockingQueue<>(createStringConsumers());
		// } else if (byte[].class.isAssignableFrom(nativeClass)) {
		// keying = b -> new String((byte[]) b);
		consumers = new LinkedBlockingQueue<>(createBinaryConsumers());
		// } else throw new IllegalArgumentException();
		closing(this::closeKafka);
	}

	protected final List<ConsumerIterator<String, String>> createStringConsumers() throws ConfigException {
		Map<String, List<KafkaStream<String, String>>> temp = null;
		StringDecoder d = new StringDecoder(null);
		ConsumerConnector c = null;
		do try {
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
		List<ConsumerIterator<String, String>> l = Colls.list();
		for (Entry<String, List<KafkaStream<String, String>>> e : temp.entrySet()) //
			for (KafkaStream<String, String> ks : e.getValue()) l.add(ks.iterator());
		return l;
	}

	private List<ConsumerIterator<byte[], byte[]>> createBinaryConsumers() throws ConfigException {
		Map<String, List<KafkaStream<byte[], byte[]>>> temp = null;
		ConsumerConnector c = null;
		do try {
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
		for (Entry<String, List<KafkaStream<byte[], byte[]>>> e : temp.entrySet()) //
			for (KafkaStream<byte[], byte[]> ks : e.getValue()) l.add(ks.iterator());
		return l;
	}

	@Override
	public Statistic statistic() {
		return new Statistic(this).<MessageAndMetadata<byte[], byte[]>> sizing(km -> (long) km.rawMessage$1().payloadSize()) //
				.<MessageAndMetadata<byte[], byte[]>> infoing(km -> new String(km.key())).detailing(Exeter.of()::toString);
	}

	private static final long EMPTY_INFO_ITV = Long.parseLong(Configs.gets("albatis.kafka.input.empty.info.interval", "10"));
	private static final long EMPTY_SLEEP = Long.parseLong(Configs.gets("albatis.kafka.input.empty.sleep.ms", "100"));
	private static final AtomicReference<Instant> LAST_FETCH = new AtomicReference<>(null);

	@SuppressWarnings("unchecked")
	@Override
	public Rmap dequeue() {
		ConsumerIterator<byte[], byte[]> it;
		MessageAndMetadata<byte[], byte[]> m;
		Rmap r = null;
		while (opened()) {
			if (null != (it = consumers.poll())) try {
				if (null == (m = it.next())) return null;
				MessageAndMetadata<byte[], byte[]> km = (MessageAndMetadata<byte[], byte[]>) m;
				String k = null == km.key() || km.key().length == 0 ? null : new String(km.key());
				return r = new Rmap(km.topic(), k, null == k ? "km" : k, (Object) km.message());
			} catch (ConsumerTimeoutException | NoSuchElementException ex) {
				Instant last = LAST_FETCH.get();
				if (null != last && Duration.between(Instant.now(), last).abs().getSeconds() > EMPTY_INFO_ITV) //
					logger().debug("No data (kafka timeout) for more than " + EMPTY_INFO_ITV + " seconds.");
			} catch (Exception ex) {
				logger().warn("Unprocessed kafka error [" + ex.getClass().toString() + ": " + ex.getMessage() + "], ignore and continue.", ex);
			} finally {
				consumers.offer(it);
				LAST_FETCH.set(Instant.now());
			}
			if (EMPTY_SLEEP > 0) try {
				Thread.sleep(EMPTY_SLEEP);
			} catch (InterruptedException e) {}
		}
		return r;
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