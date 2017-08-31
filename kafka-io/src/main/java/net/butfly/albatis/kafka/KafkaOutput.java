package net.butfly.albatis.kafka;

import static net.butfly.albacore.utils.collection.Streams.list;
import static net.butfly.albacore.utils.collection.Streams.of;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.EnqueueException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

/**
 * @author zx
 */
public final class KafkaOutput extends Namedly implements Output<Message> {
	private final URISpec uri;
	private final KafkaOutputConfig config;
	private final Producer<byte[], byte[]> producer;
	private final Function<Map<String, Object>, byte[]> coder;

	public KafkaOutput(String name, URISpec kafkaURI, Function<Map<String, Object>, byte[]> coder) throws ConfigException {
		super(name);
		this.coder = coder;
		uri = kafkaURI;
		config = new KafkaOutputConfig(uri);
		producer = new Producer<byte[], byte[]>(config.getConfig());
		closing(producer::close);
		open();
	}

	@Override
	public long enqueue(Stream<Message> messages) {
		List<Message> msgs = list(messages);
		List<KeyedMessage<byte[], byte[]>> ms = list(of(msgs).map(m -> Kafkas.toKeyedMessage(m, coder)));
		try {
			if (!ms.isEmpty()) producer.send(ms);
			return ms.size();
		} catch (Exception e) {
			EnqueueException ex = new EnqueueException();
			of(msgs).forEach(m -> ex.fail(m, e));
			throw ex;
		}
	}

	public String getDefaultTopic() {
		return config.topics().isEmpty() ? null : config.topics().get(0);
	}
}
