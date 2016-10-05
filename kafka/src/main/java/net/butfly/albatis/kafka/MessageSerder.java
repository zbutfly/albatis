package net.butfly.albatis.kafka;

import java.io.IOException;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.serder.Serder;

public class MessageSerder implements Serder<Message, byte[]> {
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
		try {
			return (T) new Message(from);
		} catch (IOException e) {
			return null;
		}
	}
}
