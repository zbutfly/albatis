package net.butfly.albatis.kafka;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.TypeToken;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.butfly.albacore.io.Input;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albatis.kafka.backend.InputThread;
import net.butfly.albatis.kafka.backend.Queue;
import net.butfly.albatis.kafka.backend.Queue.Message;
import net.butfly.albatis.kafka.config.KafkaInputConfig;
import scala.Tuple3;

public class KafkaInput implements Input<Message> {
	private static final long serialVersionUID = 7617065839861658802L;

	private static final Logger logger = LoggerFactory.getLogger(KafkaInput.class);

	private String name;
	private ConsumerConnector connect;
	private Queue context;
	private ExecutorService threads;
	private BsonSerder serder;
	private long batchSize;

	/**
	 * @param poolSize
	 * @param batchSize
	 * @param mixed:
	 *            是否混合模式
	 * @return
	 */
	public KafkaInput(String name, final KafkaInputConfig config, final Map<String, Integer> topics, long poolSize, long batchSize)
			throws KafkaException {
		super();
		this.batchSize = batchSize;
		this.name = name;
		this.serder = new BsonSerder();
		int total = 0;
		for (Integer t : topics.values())
			if (null != t) total += t;
		if (total == 0) throw new KafkaException("Kafka configuration has no topic definition.");
		threads = Executors.newFixedThreadPool(topics.size());
		logger.info("[" + name + "] Consumer thread starting (max: " + total + ")...");
		context = new Queue(curr(config.toString()), poolSize);
		createConsumers(config.getConfig(), topics);
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
			l.add(new Tuple3<String, byte[], Map<String, Object>>(message.getTopic(), message.getKey(), serder.der(message.getMessage(),
					T_MAP)));
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
		connect.commitOffsets();
	}

	@Override
	public void close() {
		threads.shutdown();
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

	private void createConsumers(ConsumerConfig config, Map<String, Integer> topics) {
		long c = 0;
		try {
			connect = Consumer.createJavaConsumerConnector(config);
			Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = connect.createMessageStreams(topics);
			for (List<KafkaStream<byte[], byte[]>> streams : streamMap.values())
				if (!streams.isEmpty()) for (final KafkaStream<byte[], byte[]> stream : streams) {
					threads.submit(new InputThread(context, stream, batchSize, this::commit));
					c++;
				}
		} catch (Exception e) {
			throw new RuntimeException("Kafka connecting failure.", e);
		}
		logger.info("[" + name + "] Consumer thread started (current: " + c + ").");
	}
}
