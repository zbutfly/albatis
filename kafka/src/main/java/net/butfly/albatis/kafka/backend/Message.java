package net.butfly.albatis.kafka.backend;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

import com.google.common.base.Charsets;

public class Message implements Serializable {
	private static final long serialVersionUID = -8599938670114294267L;
	private String topic;
	private byte[] key;
	private byte[] message;

	public Message(String topic, byte[] key, byte[] message) {
		super();
		this.topic = topic;
		this.key = key;
		this.message = message;
	}

	public Message(byte[] data) {
		try (ByteArrayInputStream bo = new ByteArrayInputStream(data)) {
			topic = new String(read(bo), Charsets.UTF_8);
			key = read(bo);
			message = read(bo);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public String getTopic() {
		return topic;
	}

	public byte[] getKey() {
		return key;
	}

	public byte[] getMessage() {
		return message;
	}

	byte[] toBytes() {
		try (ByteArrayOutputStream bo = new ByteArrayOutputStream()) {
			write(bo, topic.getBytes(Charsets.UTF_8));
			write(bo, key);
			write(bo, message);
			return bo.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void write(ByteArrayOutputStream os, byte[] data) throws IOException {
		if (null == data) os.write(-1);
		else {
			os.write(data.length);
			if (data.length > 0) os.write(data);
		}
	}

	private static byte[] read(ByteArrayInputStream bo) throws IOException {
		int len = bo.read();
		if (len == -1) return null;
		if (len == 0) return new byte[0];
		byte[] data = new byte[len];
		if (bo.read(data) != len) throw new RuntimeException();
		return data;
	}
}
