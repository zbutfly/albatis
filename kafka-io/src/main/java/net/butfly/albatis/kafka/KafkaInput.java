package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.MapInputImpl;
import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.config.KafkaInputConfig;

public class KafkaInput extends MapInputImpl<String, KafkaMessage> {
	private static final long serialVersionUID = 7617065839861658802L;
	private static final Logger logger = Logger.getLogger(KafkaInput.class);
	private final ConsumerConnector connect;
	private final Map<String, List<KafkaStream<byte[], byte[]>>> topicStreams;
	// private KafkaConsumer<byte[], byte[]> consumer;

	public KafkaInput(String name, final String config, final Map<String, Integer> topics) throws ConfigException, IOException {
		this(name, new KafkaInputConfig(config), topics);
	}

	public KafkaInput(String name, final KafkaInputConfig config, final Map<String, Integer> topics) throws ConfigException {
		super(name);
		connect = connect(config.getConfig());

		long fucking = Long.parseLong(System.getProperty("albatis.kafka.fucking.waiting", "15000"));

		this.topicStreams = connect.createMessageStreams(topics);
		logger.error("FFFFFFFucking lazy initialization of Kafka, we are sleeping [" + fucking + "ms].");
		Concurrents.waitSleep(fucking);
		logger.error("We had waked up, are you ok, Kafka?");
		logger.debug("KafkaInput ready in [" + topicStreams.size() + "] topics: " + topics.toString());
	}

	@Override
	public List<KafkaMessage> dequeue(long batchSize, String key) {
		List<KafkaMessage> r = new ArrayList<>();
		Iterator<KafkaStream<byte[], byte[]>> streamItor = topicStreams.get(key).iterator();
		while (streamItor.hasNext()) {
			ConsumerIterator<byte[], byte[]> it = streamItor.next().iterator();
			while (it.hasNext()) {
				r.add(new KafkaMessage(it.next()));
				if (r.size() >= batchSize) return r;
			}
		}
		return r;
	}

	@Override
	public boolean empty(String key) {
		return false;
	}

	@Override
	public long size() {
		return Long.MAX_VALUE;
	}

	@Override
	public long size(String key) {
		return Long.MAX_VALUE;
	}

	@Override
	public Set<String> keys() {
		return topicStreams.keySet();
	}

	@Override
	public Q<Void, KafkaMessage> q(String key) {
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
}
