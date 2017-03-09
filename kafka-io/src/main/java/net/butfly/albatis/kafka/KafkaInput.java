package net.butfly.albatis.kafka;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import com.bluejeans.bigqueue.BigQueue;

import kafka.common.ConsumerRebalanceFailedException;
import kafka.common.MessageStreamsExistException;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.InputImpl;
import net.butfly.albacore.io.OpenableThread;
import net.butfly.albacore.io.Streams;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.parallel.Concurrents;
import net.butfly.albatis.kafka.config.KafkaInputConfig;

public final class KafkaInput extends InputImpl<KafkaMessage> {
	private static final long POOL_LOCK_WAITING = -1;
	private final KafkaInputConfig config;
	private final Map<String, Integer> allTopics = new ConcurrentHashMap<>();
	private final ConsumerConnector connect;
	private final Map<KafkaStream<byte[], byte[]>, Fetcher> raws;
	private final BigQueue pool;
	private final long poolSize;
	// for debug
	private final AtomicLong skip;

	public KafkaInput(String name, String kafkaURI, String poolPath, String... topics) throws ConfigException, IOException {
		this(name, new URISpec(kafkaURI), poolPath, topics);
	}

	public KafkaInput(String name, URISpec kafkaURI, String poolPath) throws ConfigException, IOException {
		this(name, kafkaURI, poolPath, Texts.split(kafkaURI.getParameter("topic", ""), ",").toArray(new String[0]));
	}

	public KafkaInput(String name, URISpec uri, String poolPath, String... topics) throws ConfigException, IOException {
		super(name);
		config = new KafkaInputConfig(name(), uri);
		skip = new AtomicLong(Long.parseLong(uri.getParameter("skip", "0")));
		if (skip.get() > 0) logger().error("Skip [" + skip.get()
				+ "] for testing, the skip is estimated, especially in multiple topic subscribing.");
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
		// connect
		Map<String, List<KafkaStream<byte[], byte[]>>> temp = null;
		ConsumerConnector c = null;
		do
			try {
				c = kafka.consumer.Consumer.createJavaConsumerConnector(config.getConfig());
				temp = c.createMessageStreams(allTopics);
			} catch (ConsumerRebalanceFailedException | MessageStreamsExistException e) {
				logger().warn("Kafka reopen too quickly, wait 10 seconds and retry");
				if (c != null) c.shutdown();
				temp = null;
				if (!Concurrents.waitSleep(1000 * 10)) throw e;
			}
		while (temp == null);
		connect = c;
		logger().debug("connected.");
		Stream<KafkaStream<byte[], byte[]>> r = Streams.of(temp.values()).flatMap(t -> Streams.of(t));
		// connect ent
		poolSize = config.getPoolSize();
		try {
			pool = new BigQueue(IOs.mkdirs(poolPath + "/" + name), config.toString());
		} catch (IOException e) {
			throw new RuntimeException("Offheap pool init failure", e);
		}
		AtomicInteger findex = new AtomicInteger();
		raws = IO.map(r, s -> s, s -> new Fetcher(name + "Fetcher", s, findex.incrementAndGet()));
		logger().info(MessageFormat.format("[{0}] local pool init: [{1}/{0}] with name [{2}], init size [{3}].", name, poolPath, config
				.toString(), pool.size()));
		closing(this::closeKafka);
		open();
	}

	@Override
	public void open() {
		for (Fetcher f : raws.values())
			f.open();
		super.open();
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
			pool.close();
		} catch (IOException e) {
			logger().error("local pool close failure", e);
		}
	}

	public long poolSize() {
		return pool.size();
	}

	class Fetcher extends OpenableThread {
		private final ConsumerIterator<byte[], byte[]> it;

		public Fetcher(String inputName, KafkaStream<byte[], byte[]> stream, int i) {
			super(inputName + "#" + i);
			this.it = stream.iterator();
			this.setUncaughtExceptionHandler((t, e) -> logger().error("[" + getName() + "] async error, pool [" + pool.size() + "]", e));
		}

		private boolean hasNext() {
			while (true)
				try {
					return it.hasNext();
				} catch (ConsumerTimeoutException ex) {
					Concurrents.waitSleep(POOL_LOCK_WAITING);
				}
		}

		@Override
		protected void exec() {
			skip(skip, 100000);
			while (opened())
				try {
					while (opened() && hasNext()) {
						while (opened() && pool.size() > poolSize)
							Concurrents.waitSleep();
						byte[] b = new KafkaMessage(it.next()).toBytes();
						pool.enqueue(b);
					}
					Concurrents.waitSleep(1000); // kafka empty
				} catch (Exception e) {
					logger().warn("Kafka reading failed and ignore...", e);
				} finally {
					pool.gc();
				}
			logger().info("Fetcher finished and exited, pool [" + pool.size() + "].");
		}

		private long skip(AtomicLong skip, long logStep) {
			if (skip.get() <= 0) return 0;
			int skiped = 0;
			while (opened() && skip.decrementAndGet() > 0 && hasNext()) {
				it.next();
				if ((++skiped) % logStep == 0) logger().error("Skip " + logStep + " messages on stream");
			}
			logger().error("Skip " + skiped % logStep + " messages on stream");
			return skiped;
		}
	}

	@Override
	protected KafkaMessage dequeue() {
		byte[] buf = null;
		buf = pool.dequeue();
		if (null == buf) return null;
		return new KafkaMessage(buf);
	}
}
