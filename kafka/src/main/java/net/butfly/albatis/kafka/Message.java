package net.butfly.albatis.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.serder.Serder;
import net.butfly.albacore.utils.IOs;

public class Message implements Serializable {
	private static final long serialVersionUID = -8599938670114294267L;
	public static final TypeToken<Message> TOKEN = TypeToken.of(Message.class);
	public static final Serder<Message, byte[]> SERDER = new Serder<Message, byte[]>() {
		private static final long serialVersionUID = 7305698265437469539L;

		@Override
		public <T extends Message> byte[] ser(T from) {
			try {
				return from.toBytes();
			} catch (IOException e) {
				return null;
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T extends Message> T der(byte[] from, TypeToken<T> to) {
			return (T) new Message(from);
		}
	};
	public static final Converter<byte[], String> KEYING = m -> SERDER.der(m, Message.TOKEN).getTopic();
	public static final Converter<Message, Message> C = t -> t;

	private String topic;
	private byte[] key;
	private byte[] body;

	public Message(String topic, byte[] key, byte[] body) {
		super();
		this.topic = topic;
		this.key = key;
		this.body = body;
	}

	Message(byte[] data) {
		super();
		try (ByteArrayInputStream bo = new ByteArrayInputStream(data)) {
			topic = new String(IOs.readBytes(bo), Charsets.UTF_8);
			key = IOs.readBytes(bo);
			body = IOs.readBytes(bo);
		} catch (IOException e) {}
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

	byte[] toBytes() throws IOException {
		try (ByteArrayOutputStream bo = new ByteArrayOutputStream()) {
			IOs.writeBytes(bo, topic.getBytes(Charsets.UTF_8), key, body);
			return bo.toByteArray();
		}
	}
}