package net.butfly.albatis.kafka;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albacore.paral.Sdream.of1;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.kafka.config.Kafka2OutputConfig;

public class Kafka2Output extends KafkaOut {
	private static final long serialVersionUID = -7619558227408835825L;
	private final URISpec uri;
	private final Kafka2OutputConfig config;
	private final Producer<byte[], byte[]> producer;

	public Kafka2Output(String name, URISpec kafkaURI) throws ConfigException {
		super(name);
		uri = kafkaURI;
		config = new Kafka2OutputConfig(uri);
		producer = new KafkaProducer<byte[], byte[]>(config.props());
		// producer = new KafkaProducer<byte[], byte[]>(/* config. */props());
		closing(producer::close);
	}

	protected static Properties props() {
		Properties props = new Properties();
		// props.put("schema.registry.url", KafkaDemo.SCHEMA_REG);
		props.put("bootstrap.servers", "10.41.2.68:9092");
		props.put("key.serializer", ByteArraySerializer.class.getName());
		props.put("value.serializer", ByteArraySerializer.class.getName());
		return props;
	}

	@Override
	protected void enqsafe(Sdream<Rmap> messages) {
		List<Rmap> msgs = messages.list();
		Map<ProducerRecord<byte[], byte[]>, Rmap> ms = Maps.of();
		of(msgs).eachs(r -> r.forEach((k, body) -> ms.put(new ProducerRecord<>(r.table().name, k.getBytes(), (byte[]) body), r)));
		if (!ms.isEmpty()) ms.forEach(this::send);
	}

	private void send(ProducerRecord<byte[], byte[]> m, Rmap r) {
		// System.err.println("===> 1 sending");
		try {
			producer.send(m, (meta, err) -> callback(meta, err, m, r));
		} catch (Exception e) {
			failed(of1(r));
		} finally {}
	}

	protected final void callback(RecordMetadata meta, Exception err, ProducerRecord<byte[], byte[]> m, Rmap r) {
		// System.err.println("<=== 1 sent");
		if (null == err) succeeded(1);// maybe error
		else failed(of1(r));
	}

	public String getDefaultTopic() {
		return config.topics().isEmpty() ? null : config.topics().get(0);
	}

	@Override
	public URISpec target() {
		return uri;
	}
}
