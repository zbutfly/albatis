package net.butfly.albatis.kafka;

import static net.butfly.albacore.paral.Sdream.of;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
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

	private static AtomicLong count = new AtomicLong(), spent = new AtomicLong();

	@Override
	protected void enqsafe(Sdream<Rmap> messages) {
		List<Rmap> msgs = messages.list();
		List<KeyedMessage<byte[], byte[]>> ms = of(msgs).mapFlat(m -> {
			List<KeyedMessage<byte[], byte[]>> l = Colls.list();
			m.forEach((k, body) -> l.add(new KeyedMessage<>(m.table().table, k.getBytes(), (byte[]) body)));
			return of(l);
		}).nonNull().list();
		if (!ms.isEmpty()) try {
			int s = ms.size();
			long now = System.currentTimeMillis();
			try {
				if (s == 1) producer.send(ms.get(0));
				else producer.send(ms);
			} finally {
				long t = spent.addAndGet(System.currentTimeMillis() - now) / 1000;
				long c = count.addAndGet(s);
				if (t > 1 && c % 1000 == 0) //
					logger().trace("\n\tKafka sent [" + c + " recs], spent [" + t + " s], avg [" + c / t + " recs/s].");
			}
			succeeded(s);
		} catch (

		Exception e) {
			failed(Sdream.of(msgs));
		}
	}

	public String getDefaultTopic() {
		return config.topics().isEmpty() ? null : config.topics().get(0);
	}

	@Override
	public URISpec target() {
		return uri;
	}
}
