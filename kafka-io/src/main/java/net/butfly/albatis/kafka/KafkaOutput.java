package net.butfly.albatis.kafka;

import static net.butfly.albacore.utils.collection.Streams.list;
import static net.butfly.albacore.utils.collection.Streams.map;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

/**
 * @author zx
 */
public final class KafkaOutput extends OutputBase<Message> {
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
	}

	@Override
	public void enqueue(Stream<Message> messages) {
		List<Message> msgs = list(messages);
		List<KeyedMessage<byte[], byte[]>> ms = map(msgs, m -> Kafkas.toKeyedMessage(m, coder), Collectors.toList());
		if (!ms.isEmpty()) try {
			producer.send(ms);
			succeeded(ms.size());
		} catch (Exception e) {
			failed(msgs.parallelStream());
		}
	}

	public String getDefaultTopic() {
		return config.topics().isEmpty() ? null : config.topics().get(0);
	}
}
