package net.butfly.albatis.kafka;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Stream;

import kafka.common.ConsumerRebalanceFailedException;
import kafka.common.MessageStreamsExistException;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.utils.Its;
import net.butfly.albacore.io.utils.Streams;
import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.parallel.Concurrents;
import net.butfly.albatis.kafka.config.KafkaInputConfig;

public final class KafkaInput0 extends Namedly implements Input<KafkaMessage> {
	private KafkaInputConfig config;
	private final Map<String, Integer> allTopics = new ConcurrentHashMap<>();
	private final Map<ConsumerIterator<byte[], byte[]>, ReentrantLock> locks = new ConcurrentHashMap<>();
	private ConsumerConnector connect;
	private final List<ConsumerIterator<byte[], byte[]>> raws;

	public KafkaInput0(String name, String kafkaURI, String... topics) throws ConfigException, IOException {
		this(name, new URISpec(kafkaURI), topics);
	}

	public KafkaInput0(String name, URISpec kafkaURI) throws ConfigException, IOException {
		this(name, kafkaURI, Texts.split(kafkaURI.getParameter("topic", ""), ",").toArray(new String[0]));
	}

	public KafkaInput0(String name, URISpec uri, String... topics) throws ConfigException, IOException {
		super(name);
		config = new KafkaInputConfig(name(), uri);
		int kp = config.getPartitionParallelism();
		logger().info("connecting with config [" + config.toString() + "].");
		if (topics == null || topics.length == 0) topics = config.topics().toArray(new String[0]);
		Map<String, int[]> topicParts;
		try (ZKConn zk = new ZKConn(config.getZookeeperConnect())) {
			topicParts = zk.getTopicPartitions(topics);
			for (String t : topics)
				logger().debug(() -> "Lag of " + config.getGroupId() + "@" + t + ": " + zk.getLag(t, config.getGroupId()));
		}
		for (String t : topicParts.keySet()) {
			if (kp <= 0) allTopics.put(t, 1);
			else if (kp >= topicParts.get(t).length) allTopics.put(t, topicParts.get(t).length);
			else allTopics.put(t, (int) Math.ceil(topicParts.get(t).length * 1.0 / kp));
		}
		logger().debug("parallelism of topics: " + allTopics.toString() + ".");
		Stream<ConsumerIterator<byte[], byte[]>> r = connect();
		raws = IO.list(r);
		logger().info(MessageFormat.format("[{0}] local pool init: [{1}/{0}] with name [{2}], init size [{3}].", name, config.toString()));
		closing(this::closeKafka);
		open();
	}

	private Stream<ConsumerIterator<byte[], byte[]>> connect() throws ConfigException {
		Map<String, List<KafkaStream<byte[], byte[]>>> temp = null;
		do
			try {
				connect = kafka.consumer.Consumer.createJavaConsumerConnector(config.getConfig());
				temp = connect.createMessageStreams(allTopics);
			} catch (ConsumerRebalanceFailedException | MessageStreamsExistException e) {
				logger().warn("Kafka reopen too quickly, wait 10 seconds and retry");
				connect.shutdown();
				temp = null;
				if (!Concurrents.waitSleep(1000 * 10)) throw e;
			}
		while (temp == null);
		logger().debug("connected.");
		return Streams.of(temp.values()).flatMap(t -> Streams.of(t).map(s -> s.iterator()));
	}

	private void closeKafka() {
		try {
			connect.commitOffsets(true);
		} catch (Exception e) {
			logger().error("kafka commit fail", e);
		}
		try {
			connect.shutdown();
		} catch (Exception e) {
			logger().error("kafka shutdown fail", e);
		}
	}

	@Override
	public final long dequeue(Function<Stream<KafkaMessage>, Long> using, long batchSize) {
		if (!opened() || raws.isEmpty()) return 0;
		List<ConsumerIterator<byte[], byte[]>> l = new ArrayList<>(raws);
		Collections.shuffle(l);
		Iterator<ConsumerIterator<byte[], byte[]>> sit = Its.loop(l);
		AtomicReference<ConsumerIterator<byte[], byte[]>> curr = new AtomicReference<>(sit.next());
		return using.apply(Streams.of(() -> {
			ConsumerIterator<byte[], byte[]> begin = curr.get(), it = begin;
			KafkaMessage km = null;
			while (opened() && null == km) {
				while (!hasNext(it) && sit.hasNext())
					// one whole loop, return for waiting next;
					if (begin == (it = sit.next())) return tryNext(it, i -> new KafkaMessage(i.next()));
				km = tryNext(it, i -> new KafkaMessage(i.next()));
			}
			curr.lazySet(it);
			return km;
		}, batchSize, () -> opened()));
	}

	private boolean hasNext(ConsumerIterator<byte[], byte[]> it) {
		Boolean b = tryNext(it, i -> i.hasNext());
		return null == b ? false : b.booleanValue();
	}

	private <V> V tryNext(ConsumerIterator<byte[], byte[]> it, Function<ConsumerIterator<byte[], byte[]>, V> using) {
		ReentrantLock lock = locks.computeIfAbsent(it, i -> new ReentrantLock());
		if (lock.tryLock()) try {
			if (it.hasNext()) return using.apply(it);
		} finally {
			lock.unlock();
		}
		return null;
	}

	@Deprecated
	protected KafkaMessage dequeue() {
		if (!opened()) return null;
		do {
			for (ConsumerIterator<byte[], byte[]> it : raws) {
				ReentrantLock lock = locks.computeIfAbsent(it, i -> new ReentrantLock());
				if (lock.tryLock()) try {
					if (it.hasNext()) return new KafkaMessage(it.next());
				} finally {
					lock.unlock();
				}
			}
		} while (opened() && Concurrents.waitSleep());
		return null;
	}
}
