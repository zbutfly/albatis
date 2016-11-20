package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.InputQueue;
import net.butfly.albacore.io.MapQueueImpl;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.config.KafkaInputConfig;

public class KafkaInput extends MapQueueImpl<String, Void, KafkaMessage> {
	private static final long serialVersionUID = 7617065839861658802L;
	private static final Logger logger = Logger.getLogger(KafkaInput.class);
	private final ConsumerConnector connect;

	public KafkaInput(String name, final String config, final Map<String, Integer> topics) throws ConfigException, IOException {
		this(name, new KafkaInputConfig(config), topics);
	}

	public KafkaInput(String name, final KafkaInputConfig config, final Map<String, Integer> topics) throws ConfigException {
		super(name, 0);
		connect = connect(config.getConfig());
		Map<String, InputQueue<KafkaMessage>> map = streaming(topics, Long.parseLong(System.getProperty("albatis.kafka.fucking.waiting",
				"15000")));
		initialize(map);
	}

	@Override
	protected String keying(Void e) {
		throw new UnsupportedOperationException();
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

	private Map<String, InputQueue<KafkaMessage>> streaming(Map<String, Integer> topics, long fucking) {
		Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = connect.createMessageStreams(topics);
		logger.error("FFFFFFFucking lazy initialization of Kafka, we are sleeping [" + fucking + "ms].");
		Concurrents.waitSleep(fucking);
		logger.error("We had waked up, are you ok, Kafka?");
		logger.debug("KafkaInput ready in [" + streamMap.size() + "] topics: " + topics.toString());

		Map<String, InputQueue<KafkaMessage>> streamings = new HashMap<>();
		for (String topic : streamMap.keySet()) {
			List<KafkaStream<byte[], byte[]>> streams = streamMap.get(topic);
			if (!streams.isEmpty()) for (final KafkaStream<byte[], byte[]> stream : streams) {
				KafkaTopicInput q = new KafkaTopicInput(topic, stream.iterator(), connect::commitOffsets);
				streamings.put(topic, q);
			}
		}
		return streamings;
	}
}
