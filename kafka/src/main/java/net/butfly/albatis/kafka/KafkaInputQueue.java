package net.butfly.albatis.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.InputQueue;
import net.butfly.albacore.io.MapQueueImpl;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.config.KafkaInputConfig;

public class KafkaInputQueue extends MapQueueImpl<String, Void, Message, MessageAndMetadata<byte[], byte[]>> {
	private static final long serialVersionUID = 7617065839861658802L;
	private static final Logger logger = Logger.getLogger(KafkaInputQueue.class);
	private final ConsumerConnector connect;

	public KafkaInputQueue(final KafkaInputConfig config, final Map<String, Integer> topics) throws ConfigException {
		super("kafka-input-queue", -1L);
		connect = connect(config.getConfig());
		Map<String, InputQueue<Message>> map = streaming(topics, Long.parseLong(System.getProperty("albatis.kafka.fucking.waiting",
				"15000")));
		initialize(map);
	}

	@Override
	protected String keying(Void e) {
		return null;
	}

	@Override
	public long size() {
		return Long.MAX_VALUE;
	}

	@Override
	public void close() {
		connect.shutdown();
	}

	private ConsumerConnector connect(ConsumerConfig config) {
		logger.debug("Kafka [" + config.zkConnect() + "] connecting (groupId: [" + config.groupId() + "]).");
		ConsumerConnector conn = Consumer.createJavaConsumerConnector(config);
		logger.debug("Kafka [" + config.zkConnect() + "] Connected (groupId: [" + config.groupId() + "]).");
		return conn;
	}

	private Map<String, InputQueue<Message>> streaming(Map<String, Integer> topics, long fucking) {
		Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = connect.createMessageStreams(topics);
		logger.error("FFFFFFFucking lazy initialization of Kafka, we are sleeping [" + fucking + "ms].");
		Concurrents.waitSleep(fucking);
		logger.error("We had waked up, are you ok, Kafka?");
		logger.debug("Kafka ready in [" + streamMap.size() + "] topics.");

		Map<String, InputQueue<Message>> streamings = new HashMap<>();
		for (String topic : streamMap.keySet()) {
			List<KafkaStream<byte[], byte[]>> streams = streamMap.get(topic);
			logger.debug("Kafka (topic: [" + topic + "]) in [" + streams.size() + "] streams.");
			if (!streams.isEmpty()) for (final KafkaStream<byte[], byte[]> stream : streams) {
				logger.debug("Kafka thread (topic: [" + topic + "]) is creating... ");
				streamings.put(topic, new KafkaTopicInputQueue(topic, connect, stream.iterator()));
			}
		}
		return streamings;
	}
}
