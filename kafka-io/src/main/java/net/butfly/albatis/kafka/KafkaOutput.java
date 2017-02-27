package net.butfly.albatis.kafka;

import java.util.List;
import java.util.stream.Stream;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Exceptions;
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
		open();
	}

	@Override
	public void close() {
		Output.super.close(producer::close);
	}

	@Override
	public long enqueue(Stream<KafkaMessage> messages) {
		List<KeyedMessage<byte[], byte[]>> ms = IO.list(messages.map(KafkaMessage::toKeyedMessage));
		try {
			producer.send(ms);
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
