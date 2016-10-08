package net.butfly.albatis.kafka.deprecated;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.MapQueueImpl;
import net.butfly.albacore.io.OffHeapQueueImpl;
import net.butfly.albacore.io.deprecated.Input;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.Message;
import net.butfly.albatis.kafka.config.KafkaInputConfig;

@Deprecated
public class KafkaInput implements Input<Message> {
	private static final long serialVersionUID = 7617065839861658802L;
	private static final Logger logger = Logger.getLogger(KafkaInput.class);

	private final long batchSize;
	private final ConsumerConnector connect;
	private final MapQueueImpl<String, byte[], byte[], byte[], OffHeapQueueImpl<byte[], byte[]>> context;

	private AtomicBoolean closed;

	public KafkaInput(final KafkaInputConfig config, final Map<String, Integer> topics) throws ConfigException {
		super();
		closed = new AtomicBoolean(false);
		this.batchSize = config.getBatchSize();
		int total = 0;
		for (Integer t : topics.values())
			if (null != t) total += t;
		if (total == 0) throw new ConfigException("Kafka configuration has no topic definition.");
		logger.trace("Reading threads pool created (max: " + total + ").");
		String folder = curr(config.getQueuePath(), config.toString());
		Map<String, OffHeapQueueImpl<byte[], byte[]>> queues = new HashMap<String, OffHeapQueueImpl<byte[], byte[]>>();
		for (String topic : topics.keySet())
			queues.put(topic, new OffHeapQueueImpl<byte[], byte[]>(topic, folder, config.getPoolSize()) {
				private static final long serialVersionUID = -7336726017388275161L;

				@Override
				protected byte[] conv(byte[] e) {
					return e;
				}

				@Override
				protected byte[] unconv(byte[] e) {
					return e;
				}
			});
		context = new MapQueueImpl<String, byte[], byte[], byte[], OffHeapQueueImpl<byte[], byte[]>>("kafka-input", config.getPoolSize()) {
			private static final long serialVersionUID = -9171940285503286986L;

			@Override
			protected String keying(byte[] e) {
				return new Message(e).getTopic();
			}
		};
		context.initialize(queues);
		this.connect = connect(config.getConfig(), topics, Long.parseLong(System.getProperty("albatis.kafka.fucking.waiting", "15000")));
	}

	@Override
	public List<Message> reading() {
		List<Message> msgs = new ArrayList<>();
		for (byte[] b : context.dequeue(batchSize))
			msgs.add(new Message(b));
		return msgs;
	}

	@Override
	public void commit() {
		logger.trace(() -> "Kafka reading committed [" + (Systems.isDebug() ? "Dry" : "Wet") + "].");
		if (!Systems.isDebug()) connect.commitOffsets();
	}

	@Override
	public void close() {
		closed.set(true);;
		connect.shutdown();
		context.close();
	}

	private String curr(String base, String folder) throws ConfigException {
		try {
			String path = base + "/" + Systems.getMainClass().getSimpleName() + "/" + folder.replaceAll("[:/\\,]", "-");
			File f = new File(path);
			f.mkdirs();
			return f.getCanonicalPath();
		} catch (IOException e) {
			throw new ConfigException(e);
		}
	}

	private ConsumerConnector connect(ConsumerConfig config, Map<String, Integer> topics, long fucking) {
		try {
			logger.debug("Kafka [" + config.zkConnect() + "] connecting (groupId: [" + config.groupId() + "]).");
			ConsumerConnector conn = Consumer.createJavaConsumerConnector(config);
			logger.debug("Kafka [" + config.zkConnect() + "] Connected (groupId: [" + config.groupId() + "]).");
			Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = conn.createMessageStreams(topics);

			logger.error("FFFFFFFucking lazy initialization of Kafka, we are sleeping [" + fucking + "ms].");
			Concurrents.waitSleep(fucking);
			logger.error("We had waked up, are you ok, Kafka?");

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
			List<byte[]> batch = new ArrayList<>();
			while (!closed.get()) {
				while (!iter.hasNext())
					Concurrents.waitSleep(500, logger, "Kafka service empty (topic: [" + topic + "]).");
				while (iter.hasNext()) {
					MessageAndMetadata<byte[], byte[]> meta = iter.next();
					batch.add(new Message(meta.topic(), meta.key(), meta.message()).toBytes());
					if (batch.size() > batchSize || !iter.hasNext()) {
						KafkaInput.this.commit();
						context.enqueue(batch);
						batch.clear();
					}
				}
			}
		}
	}
}
