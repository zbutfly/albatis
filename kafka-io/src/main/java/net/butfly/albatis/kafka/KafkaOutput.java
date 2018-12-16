package net.butfly.albatis.kafka;

import static net.butfly.albacore.paral.Sdream.of;

import java.util.List;
import java.util.Map;

import com.hzcominfo.albatis.nosql.Connection;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

/**
 * @author zx
 */
public class KafkaOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = -7619558227408835825L;
	private final URISpec uri;
	private final KafkaOutputConfig config;
	private final Producer<byte[], byte[]> producer;
	private Function<Map<String, Object>, byte[]> coder;

	public KafkaOutput(String name, URISpec kafkaURI) throws ConfigException {
		super(name);
		this.coder = Connection.uriser(kafkaURI);
		uri = kafkaURI;
		config = new KafkaOutputConfig(uri);
		producer = new Producer<byte[], byte[]>(config.getConfig());
		closing(producer::close);
	}

	@Override
	protected void enqsafe(Sdream<Rmap> messages) {
		List<Rmap> msgs = messages.list();
		List<KeyedMessage<byte[], byte[]>> ms = of(msgs).map(m -> Kafkas.toKeyedMessage(m, coder)).nonNull().list();
		if (!ms.isEmpty()) try {
			int s = ms.size();
			if (s == 1) {
				producer.send(ms.get(0));
				logger().trace(() -> "kafka writing success: [" + new String(ms.get(0).key()) + "] -> " + ms.get(0).toString());
			} else {
				producer.send(ms);
				for (KeyedMessage<byte[], byte[]> mm : ms)
					logger().trace(() -> "kafka writing success: [" + new String(mm.key()) + "] -> " + mm.toString());
			}
			succeeded(s);
		} catch (Exception e) {
			failed(Sdream.of(msgs));
		}
	}

	public String getDefaultTopic() {
		return config.topics().isEmpty() ? null : config.topics().get(0);
	}

	public void coder(Function<Map<String, Object>, byte[]> c) {
		this.coder = c;
	}

	@Override
	public URISpec target() {
		return uri;
	}
}
