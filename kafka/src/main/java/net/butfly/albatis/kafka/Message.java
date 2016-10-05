package net.butfly.albatis.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

import com.google.common.base.Charsets;

import net.butfly.albacore.utils.IOs;

public class Message implements Serializable {
	private static final long serialVersionUID = -8599938670114294267L;
	private String topic;
	private byte[] key;
	private byte[] body;

	public Message(String topic, byte[] key, byte[] body) {
		super();
		this.topic = topic;
		this.key = key;
		this.body = body;
	}

	Message(byte[] data) throws IOException {
		super();
		try (ByteArrayInputStream bo = new ByteArrayInputStream(data)) {
			topic = new String(IOs.readBytes(bo), Charsets.UTF_8);
			key = IOs.readBytes(bo);
			body = IOs.readBytes(bo);
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

	byte[] toBytes() throws IOException {
		try (ByteArrayOutputStream bo = new ByteArrayOutputStream()) {
			IOs.writeBytes(bo, topic.getBytes(Charsets.UTF_8), key, body);
			return bo.toByteArray();
		}
	}
}