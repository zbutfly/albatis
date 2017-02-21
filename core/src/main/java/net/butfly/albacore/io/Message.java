package net.butfly.albacore.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public abstract class Message<P, W, MSG extends Message<P, W, MSG>> implements Serializable {
	private static final long serialVersionUID = -1L;

	protected Message() {}

	public byte[] toBytes() {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos);) {
			oos.writeObject(this);
			return baos.toByteArray();
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	abstract public P partition();

	abstract public String id();

	public abstract W forWrite();

	@SuppressWarnings("unchecked")
	public static <M extends Message<?, ?, M>> M fromBytes(byte[] bytes) {
		if (null == bytes) throw new IllegalArgumentException();
		try (ObjectInputStream oos = new ObjectInputStream(new ByteArrayInputStream(bytes));) {
			return (M) oos.readObject();
		} catch (ClassNotFoundException | IOException e) {
			throw new IllegalArgumentException(e);
		}
	}
}
