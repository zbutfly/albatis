package net.butfly.albatis.kafka;

import static net.butfly.albatis.ddl.Qualifier.qf;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import net.butfly.albacore.io.lambda.Function;
import net.butfly.albatis.io.Rmap;

public interface Kafka2s {
	static Rmap message(ConsumerRecord<byte[], byte[]> km, Function<byte[], Map<String, Object>> der) {
		return new Rmap(km.topic(), new String(km.key()), der.apply(km.value()));
	}

	static List<Rmap> messages(ConsumerRecord<byte[], byte[]> km, Function<byte[], List<Map<String, Object>>> der) {
		return der.apply(km.value()).stream().map(m -> new Rmap(qf(km.topic()), km.key(), m)).collect(Collectors.toList());
	}

	static ProducerRecord<byte[], byte[]> toProducer(Rmap m, Function<Map<String, Object>, byte[]> ser) {
		return null == m.key() ? null : new ProducerRecord<byte[], byte[]>(m.table().table, m.keyBytes(), ser.apply(m.map()));
	}

	static ProducerRecord<byte[], byte[]> toKeyedMessage(Rmap m, Function<Map<String, Object>, byte[]> ser) {
		return null == m.key() ? null : new ProducerRecord<>(m.table().table, m.keyBytes(), ser.apply(m.map()));
	}

	static List<Rmap> strMessages(ConsumerRecord<String, String> km, Function<String, List<Map<String, Object>>> der) {
		return der.apply(km.value()).stream().map(m -> new Rmap(qf(km.topic()), km.key(), m)).collect(Collectors.toList());
	}
}
