package net.butfly.albatis.kafka;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import kafka.common.ConsumerRebalanceFailedException;
import kafka.common.MessageStreamsExistException;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.InputImpl;
import net.butfly.albacore.io.OpenableThread;
import net.butfly.albacore.io.Streams;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.lambda.Consumer;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albatis.kafka.config.KafkaInputConfig;

public final class KafkaInput extends InputImpl<KafkaMessage> {
	protected KafkaInputConfig config;
	protected final Map<String, Integer> allTopics = new ConcurrentHashMap<>();
	protected ConsumerConnector connect;
	Map<KafkaStream<byte[], byte[]>, Fetcher> raws;
	private IBigQueue pool;

	public KafkaInput(String name, String kafkaURI, String poolPath, String... topics) throws ConfigException, IOException {
		this(name, new URISpec(kafkaURI), poolPath, topics);
	}

	public KafkaInput(String name, URISpec kafkaURI, String poolPath) throws ConfigException, IOException {
		this(name, kafkaURI, poolPath, Texts.split(kafkaURI.getParameter("topic", ""), ",").toArray(new String[0]));
	}

	public KafkaInput(String name, URISpec uri, String poolPath, String... topics) throws ConfigException, IOException {
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
		connect = kafka.consumer.Consumer.createJavaConsumerConnector(config.getConfig());
		Map<String, List<KafkaStream<byte[], byte[]>>> temp = null;
		do
			try {
				temp = connect.createMessageStreams(allTopics);
			} catch (ConsumerRebalanceFailedException | MessageStreamsExistException e) {
				logger().warn("Kafka close and open too quickly, wait 10 seconds and retry");
				if (!Concurrents.waitSleep(1000 * 10)) throw e;
				temp = null;
			}
		while (temp == null);
		Stream<KafkaStream<byte[], byte[]>> r = Streams.of(temp.values()).flatMap(t -> Streams.of(t));
		AtomicInteger findex = new AtomicInteger();
		raws = IO.map(r, s -> s, s -> new Fetcher(name + "Fetcher", s, findex.incrementAndGet(), pool, config.getPoolSize()));
		logger().debug("connected.");
		try {
			pool = new BigQueueImpl(IOs.mkdirs(poolPath + "/" + name), config.toString());
		} catch (IOException e) {
			throw new RuntimeException("Offheap pool init failure", e);
		}
		logger().info(MessageFormat.format("[{0}] local pool init: [{1}/{0}] with name [{2}], init size [{3}].", name, poolPath, config
				.toString(), pool.size()));
		open();
	}

	@Override
	protected KafkaMessage dequeue() {
		return fetch();
	}

	@Override
	public Stream<KafkaMessage> dequeue(long batchSize) {
		return Streams.batch(batchSize, this::fetch, () -> false);
	}

	protected KafkaMessage fetch(KafkaStream<byte[], byte[]> stream, Fetcher fetcher, Consumer<KafkaMessage> result) {
		KafkaMessage m = fetch();
		result.accept(m);
		return m;
	}

	private KafkaMessage fetch() {
		byte[] buf;
		try {
			buf = pool.dequeue();
		} catch (IOException e) {
			return null;
		}
		if (null == buf) return null;
		return new KafkaMessage(buf);
	}

	@Override
	public void close() {
		super.close(this::closeKafka);
	}

	private void closeKafka() {
		for (Fetcher f : raws.values())
			if (f instanceof AutoCloseable) try {
				((AutoCloseable) f).close();
			} catch (Exception e) {
				logger().error("internal fetcher close failure", e);
			}
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
		try {
			pool.gc();
		} catch (IOException e) {
			logger().error("local pool gc failure", e);
		}
		try {
			pool.close();
		} catch (IOException e) {
			logger().error("local pool close failure", e);
		}
	}

	public long poolSize() {
		return pool.size();
	}

	static class Fetcher extends OpenableThread {
		private final KafkaStream<byte[], byte[]> stream;
		private final IBigQueue pool;
		private final long poolSize;

		public Fetcher(String inputName, KafkaStream<byte[], byte[]> stream, int i, IBigQueue pool, long poolSize) {
			super(inputName + "#" + i);
			this.stream = stream;
			this.pool = pool;
			this.poolSize = poolSize;
			this.setUncaughtExceptionHandler((t, e) -> {
				logger().error("[" + getName() + "] async error, pool [" + pool.size() + "]", e);
			});
		}

		@Override
		protected void exec() {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (opened())
				try {
					while (opened() && it.hasNext()) {
						byte[] km = new KafkaMessage(it.next()).toBytes();
						while (opened() && pool.size() > poolSize)
							Concurrents.waitSleep();
						pool.enqueue(km);
					}
					Concurrents.waitSleep(1000); // kafka empty
				} catch (Exception e) {} finally {
					gc(pool);
				}
			logger().info("Fetcher finished and exited, pool [" + pool.size() + "].");
		}

		private void gc(IBigQueue pool) {
			try {
				pool.gc();
			} catch (IOException e) {
				logger().warn("Local pool gc() failure", e);
			}
		}
	}
}
