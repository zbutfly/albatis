package net.butfly.albatis.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.base.Charsets;

import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.utils.IOs;

public class KafkaMessage implements Serializable {
	private static final long serialVersionUID = -8599938670114294267L;

	private final String topic;
	private final byte[] key;
	private final byte[] body;

	public KafkaMessage(MessageAndMetadata<byte[], byte[]> meta) {
		this(meta.topic(), meta.key(), meta.message());
	}

	public KafkaMessage(ConsumerRecord<byte[], byte[]> record) throws Exception {
		this(record.topic(), record.key(), record.value());
	}

	public KafkaMessage(String topic, byte[] key, byte[] body) {
		this.topic = topic;
		this.key = key;
		this.body = body;
	}

	public KafkaMessage(byte[] data) {
		try (ByteArrayInputStream bo = new ByteArrayInputStream(data)) {
			topic = new String(IOs.readBytes(bo), Charsets.UTF_8);
			key = IOs.readBytes(bo);
			body = IOs.readBytes(bo);
		} catch (IOException e) {
			throw new RuntimeException();
		}
	}

	public String getTopic() {
		return topic;
	}

	public byte[] getKey() {
		return key;
	}

	public byte[] getBody() {
		return body;
	}

	public byte[] toBytes() {
		try (ByteArrayOutputStream bo = new ByteArrayOutputStream()) {
			IOs.writeBytes(bo, topic.getBytes(Charsets.UTF_8), key, body);
			return bo.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	public ProducerRecord<byte[], byte[]> toProducer() {
		return new ProducerRecord<byte[], byte[]>(topic, key, body);
	}

	public KeyedMessage<byte[], byte[]> toKeyedMessage() {
		return new KeyedMessage<>(topic, key, body);
	}

	@Override
	public String toString() {
		return topic + ":" + new String(key, Charsets.UTF_8) + "[" + body.length + "]";
	}

	public long size() {
		return body.length + key.length;
	}
}