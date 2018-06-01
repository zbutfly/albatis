package net.butfly.albatis.kafka;

import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import net.butfly.albatis.io.R;

public interface Kafkas {
	static R message(MessageAndMetadata<byte[], byte[]> km, Function<byte[], Map<String, Object>> der) {
		return new R(km.topic(), new String(km.key()), der.apply(km.message()));
	}

	static ProducerRecord<byte[], byte[]> toProducer(R m, Function<Map<String, Object>, byte[]> ser) {
		return null == m.key() ? null : new ProducerRecord<byte[], byte[]>(m.table(), m.keyBytes(), ser.apply(m.map()));
	}

	static KeyedMessage<byte[], byte[]> toKeyedMessage(R m, Function<Map<String, Object>, byte[]> ser) {
		return null == m.key() ? null : new KeyedMessage<>(m.table(), m.keyBytes(), ser.apply(m.map()));
	}
}
