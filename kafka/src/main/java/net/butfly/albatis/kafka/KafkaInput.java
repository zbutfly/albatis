package net.butfly.albatis.kafka;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.butfly.albacore.utils.logger.Logger;

import com.google.common.reflect.TypeToken;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.butfly.albacore.io.Input;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.utils.async.Tasks;
import net.butfly.albatis.kafka.backend.InputThread;
import net.butfly.albatis.kafka.backend.Queue;
import net.butfly.albatis.kafka.backend.Queue.Message;
import net.butfly.albatis.kafka.config.KafkaInputConfig;
import scala.Tuple3;

public class KafkaInput implements Input<Message> {
	private static final long serialVersionUID = 7617065839861658802L;
	private static final Logger logger = Logger.getLogger(KafkaInput.class);

	private final long batchSize;
	private final ConsumerConnector connect;
	private final Queue context;
	private final ExecutorService executor;
	private final BsonSerder serder;
	private boolean debug;

	public KafkaInput(final KafkaInputConfig config, final Map<String, Integer> topics) throws KafkaException {
		super();
		this.debug = Boolean.parseBoolean(System.getProperty("albacore.debug"));
		this.batchSize = config.getBatchSize();
		this.serder = new BsonSerder();
		int total = 0;
		for (Integer t : topics.values())
			if (null != t) total += t;
		if (total == 0) throw new KafkaException("Kafka configuration has no topic definition.");
		executor = Executors.newFixedThreadPool(topics.size());
		logger.info("Kafka input thread starting (max: " + total + ")...");
		context = new Queue(curr(config.toString()), config.getPoolSize());
		this.connect = connect(config.getConfig(), topics);
	}

	@Override
	public List<Message> read() {
		return context.dequeue(batchSize);
	}

	@SuppressWarnings("serial")
	private static final TypeToken<Map<String, Object>> T_MAP = new TypeToken<Map<String, Object>>() {};

	public List<Tuple3<String, byte[], Map<String, Object>>> read(String... topic) {
		List<Tuple3<String, byte[], Map<String, Object>>> l = new ArrayList<>();
		for (Message message : context.dequeue(batchSize, topic))
			l.add(new Tuple3<String, byte[], Map<String, Object>>(message.getTopic(), message.getKey(),
					serder.der(message.getMessage(), T_MAP)));
		return l;

	}

	public <E> List<Tuple3<String, byte[], E>> read(Class<E> cl, String... topic) {
		@SuppressWarnings("serial")
		TypeToken<E> tt = new TypeToken<E>() {};
		List<Tuple3<String, byte[], E>> l = new ArrayList<>();
		for (Message message : context.dequeue(batchSize, topic))
			l.add(new Tuple3<String, byte[], E>(message.getTopic(), message.getKey(), serder.der(message.getMessage(), tt)));
		return l;
	}

	@Override
	public void commit() {
		if (!debug) connect.commitOffsets();
	}

	@Override
	public void close() {
		Tasks.waitShutdown(executor);
		connect.shutdown();
		context.close();
	}

	private String curr(String zookeeperConnect) throws KafkaException {
		try {
			File f = new File("./" + zookeeperConnect.replaceAll("[:/\\,]", "-"));
			f.mkdirs();
			return f.getCanonicalPath();
		} catch (IOException e) {
			throw new KafkaException(e);
		}
	}

	private ConsumerConnector connect(ConsumerConfig config, Map<String, Integer> topics) {
		long c = 0;
		try {
			ConsumerConnector connect = Consumer.createJavaConsumerConnector(config);
			Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = connect.createMessageStreams(topics);
			for (List<KafkaStream<byte[], byte[]>> streams : streamMap.values())
				if (!streams.isEmpty()) for (final KafkaStream<byte[], byte[]> stream : streams) {
					executor.submit(new InputThread(context, stream, batchSize, this::commit));
					c++;
				}
			logger.info("Kafka inputting thread started (current: " + c + ").");
			return connect;
		} catch (Exception e) {
			throw new RuntimeException("Kafka connecting failure.", e);
		}
	}
}
