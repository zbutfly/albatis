package net.butfly.albatis.kafka;

import java.io.IOException;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.serder.Serder;

public class MessageSerder implements Serder<KafkaMessage, byte[]> {
	private static final long serialVersionUID = 7305698265437469539L;

	@Override
	public <T extends KafkaMessage> byte[] ser(T from) {
		try {
			return from.toBytes();
		} catch (IOException e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends KafkaMessage> T der(byte[] from, TypeToken<T> to) {
		try {
			return (T) new KafkaMessage(from);
		} catch (IOException e) {
			return null;
		}
	}
}
