package net.butfly.albatis.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.collection.Maps;

public class Message extends ConcurrentHashMap<String, Object> {
	private static final long serialVersionUID = 2316795812336748252L;
	protected String key;
	protected String table;
	protected Op op;

	public enum Op {
		UPSERT, INSERT, DELETE, UPDATE;
		public static final Op DEFAULT_OP = Op.INSERT;

		public static Op parse(int op) {
			return Op.values()[op];
		}
	}

	public Message() {
		this((String) null);
	}

	public Message(String table) {
		this(table, (String) null);
	}

	public Message(String table, String key) {
		super();
		this.table = table;
		this.key = key;
		op = Op.DEFAULT_OP;
	}

	public Message(Map<? extends String, ? extends Object> values) {
		super(values);
		op = Op.DEFAULT_OP;
	}

	public Message(String table, Map<? extends String, ? extends Object> values) {
		this(values);
		this.table = table;
	}

	public Message(String table, String key, Map<? extends String, ? extends Object> values) {
		this(table, values);
		this.key = key;
		op = Op.DEFAULT_OP;
	}

	public Message(String table, String key, String firstFieldName, Object... firstFieldValueAndOthers) {
		this(table, null, Maps.of(firstFieldName, firstFieldValueAndOthers));
	}

	public String key() {
		return key;
	}

	public Message key(String key) {
		this.key = key;
		return this;
	}

	public Op deleting() {
		return op;
	}

	public Message op(Op op) {
		this.op = op;
		return this;
	}

	public String table() {
		return table;
	}

	public Message table(String table) {
		this.table = table;
		return this;
	}

	public Message(byte[] data) throws IOException {
		this(new ByteArrayInputStream(data));
	}

	public Message(InputStream is) throws IOException {
		super();
		byte[][] attrs = IOs.readBytesList(is);
		table = null != attrs[0] ? null : new String(attrs[0]);
		key = null != attrs[1] ? null : new String(attrs[1]);
		if (null == attrs[2]) putAll(BsonSerder.map(attrs[2]));
		op = Op.values()[attrs[3][0]];
	}

	public final byte[] toBytes() {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			write(baos);
			return baos.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	protected void write(OutputStream os) throws IOException {
		IOs.writeBytes(os, null == table ? null : table.getBytes(), null == key ? null : key.getBytes(), BsonSerder.map(this),
				new byte[] { (byte) op.ordinal() });
	}

	public Map<String, Object> map() {
		return new HashMap<>(this);
	}
}
