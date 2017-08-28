package net.butfly.albacore.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.collection.Maps;

public class Message extends ConcurrentHashMap<String, Object> {
	private static final long serialVersionUID = 2316795812336748252L;
	protected String table;

	public String table() {
		return table;
	}

	public Message table(String table) {
		this.table = table;
		return this;
	}

	public Message(String table, Map<? extends String, ? extends Object> values) {
		super(values);
		this.table = table;
	}

	public Message(String table) {
		super();
		this.table = table;
	}

	public Message(Map<? extends String, ? extends Object> values) {
		this(null, values);
	}

	public Message() {
		this((String) null);
	}

	public Message(String table, String firstFieldName, Object... firstFieldValueAndOthers) {
		this(table, Maps.of(firstFieldName, firstFieldValueAndOthers));
	}

	public Message(String firstFieldName, Object... firstFieldValueAndOthers) {
		this(Maps.of(firstFieldName, firstFieldValueAndOthers));
	}

	protected void write(OutputStream os) throws IOException {
		IOs.writeBytes(os, null == table ? null : table.getBytes());
		IOs.writeBytes(os, BsonSerder.DEFAULT_MAP.ser(this));
	}

	public static Message fromBytes(byte[] b) {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(b)) {
			byte[] t = IOs.readBytes(bais);
			String table = null == t ? null : new String(t);
			Map<String, Object> map = BsonSerder.DEFAULT_MAP.der(IOs.readBytes(bais));
			return new Message(table, map);
		} catch (IOException e) {
			return null;
		}
	}

	public static Message fromBytes(InputStream os) throws IOException {
		byte[] t = IOs.readBytes(os);
		String table = null == t ? null : new String(t);
		Map<String, Object> map = BsonSerder.DEFAULT_MAP.der(IOs.readBytes(os));
		return new Message(table, map);
	}

	public final byte[] toBytes() {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			write(baos);
			return baos.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}
}
