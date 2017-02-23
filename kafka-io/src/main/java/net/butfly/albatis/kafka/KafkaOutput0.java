package net.butfly.albatis.kafka;

import java.util.List;
import java.util.stream.Stream;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.OutputImpl;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

/**
 * @author zx // * @deprecated memory leak!
 */
public final class KafkaOutput0 extends OutputImpl<KafkaMessage> {
	private final URISpec uri;
	private final KafkaOutputConfig config;
	private final Producer<byte[], byte[]> producer;

	public KafkaOutput0(final String name, final String kafkaURI) throws ConfigException {
		super(name);
		uri = new URISpec(kafkaURI);
		config = new KafkaOutputConfig(uri);
		producer = new Producer<byte[], byte[]>(config.getConfig());
		open();
	}

	@Override
	public void close() {
		super.close(producer::close);
	}

	@Override
	public boolean enqueue(KafkaMessage m) {
		if (null == m) return false;
		try {
			producer.send(m.toKeyedMessage());
			return true;
		} catch (Exception e) {
			logger().error("Kafka sending failure", Exceptions.unwrap(e));
			return false;
		}
	}

	@Override
	public long enqueue(Stream<KafkaMessage> messages) {
		List<KeyedMessage<byte[], byte[]>> ms = IO.list(messages.map(m -> m.toKeyedMessage()));
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
