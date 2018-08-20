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
public final class KafkaOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = -7619558227408835825L;
	private final URISpec uri;
	private final KafkaOutputConfig config;
	private final Producer<byte[], byte[]> producer;
	private final Function<Map<String, Object>, byte[]> coder;

	public KafkaOutput(String name, URISpec kafkaURI) throws ConfigException {
		super(name);
		this.coder = Connection.uriser(kafkaURI);
		uri = kafkaURI;
		config = new KafkaOutputConfig(uri);
		producer = new Producer<byte[], byte[]>(config.getConfig());
		closing(producer::close);
	}

	@Override
	public URISpec target() {
		return uri;
	}

	@Override
	protected void enqsafe(Sdream<Rmap> messages) {
		List<Rmap> msgs = messages.list();
		List<KeyedMessage<byte[], byte[]>> ms = of(msgs).map(m -> Kafkas.toKeyedMessage(m, coder)).nonNull().list();
		if (!ms.isEmpty()) try {
			int s = ms.size();
			if (s == 1) producer.send(ms.get(0));
			else producer.send(ms);
			succeeded(s);
		} catch (Exception e) {
			failed(Sdream.of(msgs));
		}
	}

	public String getDefaultTopic() {
		return config.topics().isEmpty() ? null : config.topics().get(0);
	}
}
