package net.butfly.albatis.kafka;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.reflect.TypeToken;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.MapQueue;
import net.butfly.albacore.io.OffHeapQueue;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.serder.support.ByteArray;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.config.KafkaInputConfig;
import scala.Tuple3;

public class KafkaInput implements Input<Message> {
	private static final long serialVersionUID = 7617065839861658802L;
	private static final Logger logger = Logger.getLogger(KafkaInput.class);

	private final long batchSize;
	private final ConsumerConnector connect;
	private final MapQueue<String, Message> context;
	// XXX: should not include!
	private final BsonSerder serder;

	private boolean debug;
	private AtomicBoolean closed;

	public KafkaInput(final KafkaInputConfig config, final Map<String, Integer> topics) throws KafkaException {
		super();
		closed = new AtomicBoolean(false);
		this.debug = Systems.isDebug();
		this.batchSize = config.getBatchSize();
		this.serder = new BsonSerder();
		int total = 0;
		for (Integer t : topics.values())
			if (null != t) total += t;
		if (total == 0) throw new KafkaException("Kafka configuration has no topic definition.");
		logger.trace("Reading threads pool created (max: " + total + ").");
		MessageSerder msg = new MessageSerder();
		String path = curr(config.getQueuePath(), config.toString());
		context = new OffHeapQueue("kafka-input", path, config.getPoolSize()).fanout(m -> m.getTopic(), topic -> new OffHeapQueue(topic,
				path, config.getPoolSize()).serder(msg.unconverter(), msg.converter()), msg.unconverter(), msg.converter());
		this.connect = connect(config.getConfig(), topics);
	}

	@Override
	public List<Message> reading() {
		return context.dequeue(batchSize);
	}

	@SuppressWarnings("serial")
	private static final TypeToken<Map<String, Object>> T_MAP = new TypeToken<Map<String, Object>>() {};

	public List<Tuple3<String, byte[], Map<String, Object>>> read(String... topic) {
		List<Tuple3<String, byte[], Map<String, Object>>> l = new ArrayList<>();
		for (Message message : context.dequeue(batchSize, topic))
			l.add(new Tuple3<>(message.getTopic(), message.getKey(), serder.der(new ByteArray(message.getBody()), T_MAP)));
		return l;

	}

	public <E> List<Tuple3<String, byte[], E>> read(Class<E> cl, String... topic) {
		@SuppressWarnings("serial")
		TypeToken<E> tt = new TypeToken<E>() {};
		List<Tuple3<String, byte[], E>> l = new ArrayList<>();
		for (Message message : context.dequeue(batchSize, topic))
			l.add(new Tuple3<String, byte[], E>(message.getTopic(), message.getKey(), serder.der(new ByteArray(message.getBody()), tt)));
		return l;
	}

	@Override
	public void commit() {
		logger.trace(() -> "Kafka reading committed [" + (debug ? "Dry" : "Wet") + "].");
		if (!debug) connect.commitOffsets();
	}

	@Override
	public void close() {
		closed.set(true);;
		connect.shutdown();
		context.close();
	}

	private String curr(String base, String folder) throws KafkaException {
		try {
			String path = base + "/" + Systems.getMainClass().getSimpleName() + "/" + folder.replaceAll("[:/\\,]", "-");
			File f = new File(path);
			f.mkdirs();
			return f.getCanonicalPath();
		} catch (IOException e) {
			throw new KafkaException(e);
		}
	}

	private ConsumerConnector connect(ConsumerConfig config, Map<String, Integer> topics) {
		try {
			logger.debug("Kafka [" + config.zkConnect() + "] connecting (groupId: [" + config.groupId() + "]).");
			ConsumerConnector conn = Consumer.createJavaConsumerConnector(config);
			logger.debug("Kafka [" + config.zkConnect() + "] Connected (groupId: [" + config.groupId() + "]).");
			Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = conn.createMessageStreams(topics);

			logger.error("FFFFFFFucking lazy initialization of Kafka, we sleeping.");
			Concurrents.waitSleep(10000);

			logger.debug("Kafka ready in [" + streamMap.size() + "] topics.");
			for (String topic : streamMap.keySet()) {
				List<KafkaStream<byte[], byte[]>> streams = streamMap.get(topic);
				logger.debug("Kafka (topic: [" + topic + "]) in [" + streams.size() + "] streams.");
				if (!streams.isEmpty()) {
					for (final KafkaStream<byte[], byte[]> stream : streams) {
						logger.debug("Kafka thread (topic: [" + topic + "]) is creating... ");
						Concurrents.submit(new InputThread(topic, stream.iterator()));
					}
				}
			}
			return conn;
		} catch (Exception e) {
			throw new RuntimeException("Kafka connecting failure.", e);
		}
	}

	class InputThread extends Thread {
		private final ConsumerIterator<byte[], byte[]> iter;
		private final String topic;

		InputThread(String topic, ConsumerIterator<byte[], byte[]> iter) {
			super();
			this.topic = topic;
			this.iter = iter;
		}

		@Override
		public void run() {
			long c = 0;
			while (!closed.get()) {
				if (!iter.hasNext()) Concurrents.waitSleep(500, logger, "Kafka service empty (topic: [" + topic + "]).");
				else {
					List<Message> batch = new ArrayList<>();
					while (iter.hasNext()) {
						MessageAndMetadata<byte[], byte[]> meta = iter.next();
						batch.add(new Message(meta.topic(), meta.key(), meta.message()));
						if (++c > batchSize || !iter.hasNext()) {
							KafkaInput.this.commit();
							context.enqueue(batch.toArray(new Message[batch.size()]));
							batch.clear();
							c = 0;
						}
					}
				}
			}
		}
	}
}
