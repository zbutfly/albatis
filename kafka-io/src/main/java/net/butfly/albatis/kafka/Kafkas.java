package net.butfly.albatis.kafka;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albatis.io.Rmap;

public interface Kafkas {
	static Rmap message(MessageAndMetadata<byte[], byte[]> km, Function<byte[], Map<String, Object>> der) {
		return new Rmap(km.topic(), new String(km.key()), der.apply(km.message()));
	}

	static List<Rmap> messages(MessageAndMetadata<byte[], byte[]> km, Function<byte[], List<Map<String, Object>>> der) {
		return der.apply(km.message()).stream().map(m -> new Rmap(km.topic(), km.key(), m)).collect(Collectors.toList());
	}

	static ProducerRecord<byte[], byte[]> toProducer(Rmap m, Function<Map<String, Object>, byte[]> ser) {
		return null == m.key() ? null : new ProducerRecord<byte[], byte[]>(m.table().name, m.keyBytes(), ser.apply(m.map()));
	}

	static KeyedMessage<byte[], byte[]> toKeyedMessage(Rmap m, Function<Map<String, Object>, byte[]> ser) {
		return null == m.key() ? null : new KeyedMessage<>(m.table().name, m.keyBytes(), ser.apply(m.map()));
	}

	static List<Rmap> strMessages(MessageAndMetadata<String, String> km, Function<String, List<Map<String, Object>>> der) {
		return der.apply(km.message()).stream().map(m -> new Rmap(km.topic(), km.key(), m)).collect(Collectors.toList());
	}
}
