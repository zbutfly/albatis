package net.butfly.albatis.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Maps;

public class R extends ConcurrentHashMap<String, Object> {
	private static final long serialVersionUID = 2316795812336748252L;
	protected Object key;
	protected String table;
	protected @Op int op;

	public @interface Op {
		static int UPSERT = 0;
		static int INSERT = 1;
		static int UPDATE = 2;
		static int DELETE = 3;// DELETE must be 3
		static int INCREASE = 4;
		static final @Op int DEFAULT = Op.UPSERT;
	}

	public static String opname(@Op int op) {
		switch (op) {
		case Op.UPSERT:
			return "UPSERT";
		case Op.INSERT:
			return "INSERT";
		case Op.UPDATE:
			return "UPDATE";
		case Op.DELETE:
			return "DELETE";
		case Op.INCREASE:
			return "INCREASE";
		}
		return null;
	}

	public R() {
		this((String) null);
	}

	public R(String table) {
		this(table, (String) null);
	}

	public R(String table, Object key) {
		super();
		this.table = table;
		this.key = key;
		op = Op.DEFAULT;
	}

	public R(Map<? extends String, ? extends Object> values) {
		super(values);
		op = Op.DEFAULT;
	}

	public R(String table, Map<? extends String, ? extends Object> values) {
		this(values);
		this.table = table;
	}

	public R(String table, Object key, Map<? extends String, ? extends Object> values) {
		this(table, values);
		this.key = key;
		op = Op.DEFAULT;
	}

	public R(String table, Pair<String, Map<String, Object>> keyAndValues) {
		this(table, keyAndValues.v1(), keyAndValues.v2());
	}

	public R(String table, Object key, String firstFieldName, Object... firstFieldValueAndOthers) {
		this(table, key, Maps.of(firstFieldName, firstFieldValueAndOthers));
	}

	public Object key() {
		return key;
	}

	public R key(Object key) {
		this.key = key;
		return this;
	}

	public @Op int op() {
		return op;
	}

	public R op(@Op int op) {
		this.op = op;
		return this;
	}

	public String table() {
		return table;
	}

	public R table(String table) {
		this.table = table;
		return this;
	}

	public R(byte[] data, Function<byte[], Map<String, ?>> conv) throws IOException {
		this(new ByteArrayInputStream(data), conv);
	}

	public R(InputStream is, Function<byte[], Map<String, ?>> conv) throws IOException {
		super();
		byte[][] attrs = IOs.readBytesList(is);
		table = null != attrs[0] ? null : new String(attrs[0]);
		key = null != attrs[1] ? null : new String(attrs[1]);
		if (null == attrs[2]) putAll(conv.apply(attrs[2]));
		op = attrs[3][0];
	}

	@Override
	public String toString() {
		return "[Table: " + table + ", Key: " + key + ", Op: " + opname(op) + "] => " + super.toString();
	}

	public final byte[] toBytes(Function<Map<String, Object>, byte[]> conv) {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			write(baos, conv);
			return baos.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	public final byte[] keyBytes() {
		if (null == key) return null;
		if (key instanceof byte[]) return (byte[]) key;
		if (key instanceof CharSequence) return key.toString().getBytes();
		return key.toString().getBytes();// XXX
	}

	protected void write(OutputStream os, Function<Map<String, Object>, byte[]> conv) throws IOException {
		IOs.writeBytes(os, null == table ? null : table.getBytes(), keyBytes(), conv.apply(this), new byte[] { (byte) op });
	}

	public Map<String, Object> map() {
		Map<String, Object> m = Maps.of();
		m.putAll(this);
		return m;
	}

	public synchronized R map(Map<String, Object> map) {
		clear();
		putAll(map);
		return this;
	}
}