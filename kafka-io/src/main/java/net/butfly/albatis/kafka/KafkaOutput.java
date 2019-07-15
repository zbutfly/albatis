package net.butfly.albatis.kafka;

import static net.butfly.albacore.paral.Sdream.of;

import java.util.List;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

public class KafkaOutput extends KafkaOut {
	private static final long serialVersionUID = -7619558227408835825L;
	private final URISpec uri;
	private final KafkaOutputConfig config;
	private final Producer<byte[], byte[]> producer;

	public KafkaOutput(String name, URISpec kafkaURI) throws ConfigException {
		super(name);
		uri = kafkaURI;
		config = new KafkaOutputConfig(uri);
		producer = new Producer<byte[], byte[]>(config.getConfig());
		closing(producer::close);
	}

	@Override
	public Statistic statistic() {
		return new Statistic(this).<KeyedMessage<byte[], byte[]>> sizing(km -> null == km ? 0 : (long) km.message().length)
		// .<KeyedMessage<byte[], byte[]>>sampling(km -> null == km ? null : new String(km.message()))
		;
	}

	@Override
	protected void enqsafe(Sdream<Rmap> messages) {
		List<Rmap> msgs = messages.list();
		List<KeyedMessage<byte[], byte[]>> ml = of(msgs).mapFlat(m -> {
			List<KeyedMessage<byte[], byte[]>> l = Colls.list();
			m.forEach((k, body) -> {
				byte[] d = body instanceof CharSequence ? ((CharSequence) body).toString().getBytes() : (byte[]) body;
				if (null != d && d.length > 0) l.add(new KeyedMessage<>(m.table().name, k.getBytes(), d));
			});
			return of(l);
		}).nonNull().list();
		int size;
		if (!ml.isEmpty()) try {
			if (1 == (size = ml.size())) producer.send(ml.get(0));
			else producer.send(ml);
			succeeded(size);
		} catch (Exception e) {
			failed(Sdream.of(msgs));
		}
	}

	public String getDefaultTopic() { return config.topics().isEmpty() ? null : config.topics().get(0); }

	@Override
	public URISpec target() {
		return uri;
	}
}
