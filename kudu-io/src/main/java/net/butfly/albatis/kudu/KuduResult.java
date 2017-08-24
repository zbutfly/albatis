package net.butfly.albatis.kudu;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

import net.butfly.albacore.io.Message;

public class KuduResult extends Message<String, Map<String, Object>, KuduResult> {
	private static final long serialVersionUID = -5843704512434056538L;
	private String table;
	private final Map<String, Object> result;

	public KuduResult(Map<String, Object> result, String table) {
		this.table = table;
		this.result = result;
	}

	public KuduResult(byte[] source) {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(source); ObjectInputStream ois = new ObjectInputStream(bais);) {
			KuduResult r = (KuduResult) ois.readObject();
			if (r == null) throw new IllegalArgumentException("Null KuduResult fetch from cache");
			this.table = r.table;
			this.result = r.result;
		} catch (IOException | ClassNotFoundException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public KuduResult table(String table) {
		this.table = table;
		return this;
	}

	@Override
	public String partition() {
		return table;
	}

	@Override
	public String toString() {
		return table.toString() + "\n\t" + result.toString();
	}

	@Override
	public Map<String, Object> forWrite() {
		return result;
	}
}
