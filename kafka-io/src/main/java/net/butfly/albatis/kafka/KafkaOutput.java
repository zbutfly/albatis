package net.butfly.albatis.kafka;

import static net.butfly.albacore.paral.Sdream.of;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.SafeOutput;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

/**
 * @author zx
 */
public final class KafkaOutput extends SafeOutput<Message> {
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
	protected void enqSafe(Sdream<Message> messages) {
		try {
			List<Message> msgs = messages.list();
			List<KeyedMessage<byte[], byte[]>> ms = of(msgs).map(m -> Kafkas.toKeyedMessage(m, coder)).list();
			if (!ms.isEmpty()) try {
				int s = ms.size();
				if (s == 1) producer.send(ms.get(0));
				else producer.send(ms);
				succeeded(s);
			} catch (Exception e) {
				failed(Sdream.of(msgs));
			}
		} finally {
			opsPending.decrementAndGet();
		}
	}

	public String getDefaultTopic() {
		return config.topics().isEmpty() ? null : config.topics().get(0);
	}
}
