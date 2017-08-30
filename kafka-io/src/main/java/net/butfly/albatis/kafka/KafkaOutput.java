package net.butfly.albatis.kafka;

import static net.butfly.albacore.utils.collection.Streams.list;

import java.util.List;
import java.util.stream.Stream;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

/**
 * @author zx
 */
public final class KafkaOutput extends Namedly implements Output<KafkaMessage> {
	private final URISpec uri;
	private final KafkaOutputConfig config;
	private final Producer<byte[], byte[]> producer;

	public KafkaOutput(final String name, final String kafkaURI) throws ConfigException {
		super(name);
		uri = new URISpec(kafkaURI);
		config = new KafkaOutputConfig(uri);
		producer = new Producer<byte[], byte[]>(config.getConfig());
		closing(producer::close);
		open();
	}

	@Override
	public long enqueue(Stream<KafkaMessage> messages) {
		List<KeyedMessage<byte[], byte[]>> ms = list(messages.map(KafkaMessage::toKeyedMessage));
		try {
			if (!ms.isEmpty()) producer.send(ms);
			return ms.size();
		} catch (Exception e) {
			logger().error("Kafka sending failure", Exceptions.unwrap(e));
			return 0;
		}
	}

	public long fails() {
		return 0;
	}

	public String getDefaultTopic() {
		return config.topics().isEmpty() ? null : config.topics().get(0);
	}
}
