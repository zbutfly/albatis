package net.butfly.albatis.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.Qualifier;

public class Rmap extends ConcurrentHashMap<String, Object> {
	private static final long serialVersionUID = 2316795812336748252L;
	public static final String RAW_KEY_FIELD = ".rawkey";

	protected Object key;
	protected String keyField;
	protected Qualifier table;
	private String tableExpr;
	protected @Op int op;

	public @interface Op {
		static int UPSERT = 0;
		static int INSERT = 1;
		static int UPDATE = 2;
		static int DELETE = 3;// DELETE must be 3
		static int INCREASE = 4;
		static final @Op int DEFAULT = Op.UPSERT;
	}

	public Rmap() {
		this((Qualifier) null);
	}

	public Rmap(byte[] data, Function<byte[], Map<String, ?>> conv) throws IOException {
		this(new ByteArrayInputStream(data), conv);
	}

	public Rmap(InputStream is, Function<byte[], Map<String, ?>> conv) throws IOException {
		super();
		byte[][] attrs = IOs.readBytesList(is);
		table = null != attrs[0] ? null : new Qualifier(new String(attrs[0]));
		key = null != attrs[1] ? null : new String(attrs[1]);
		keyField = null != attrs[2] ? null : new String(attrs[2]);
		if (null == attrs[3]) putAll(conv.apply(attrs[3]));
		op = attrs[4][0];
	}

	public Rmap(Map<? extends String, ? extends Object> values) {
		super(values);
		op = Op.DEFAULT;
	}

	public Rmap(Qualifier table) {
		this(table, (String) null);
	}

	public Rmap(Qualifier table, Object key) {
		super();
		this.table = table;
		this.key = key;
		op = Op.DEFAULT;
	}

	public Rmap(Qualifier table, Map<? extends String, ? extends Object> values) {
		this(values);
		this.table = table;
	}

	public Rmap(Qualifier table, Object key, Map<? extends String, ? extends Object> values) {
		this(table, values);
		this.key = key;
		op = Op.DEFAULT;
	}

	public Rmap(Qualifier table, Pair<String, Map<String, Object>> keyAndValues) {
		this(table, keyAndValues.v1(), keyAndValues.v2());
	}

	public Rmap(Qualifier table, Object key, String firstFieldName, Object... firstFieldValueAndOthers) {
		this(table, key, Maps.of(firstFieldName, firstFieldValueAndOthers));
	}

	public Qualifier table() {
		return table;
	}

	public Object key() {
		if (null != key) return key;
		if (null != keyField) return get(keyField);
		return null;
	}

	public Rmap key(Object key) {
		this.key = key;
		if (null != keyField && null != key) put(keyField, key);
		return this;
	}

	public String keyField() {
		return keyField;
	}

	public String tableExpr() {
		return tableExpr;
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
		IOs.writeBytes(os, null == table ? //
				null : table.qualifier.getBytes(), keyBytes(), //
				null == keyField ? null : keyField.getBytes(), //
				conv.apply(this), new byte[] { (byte) op });
	}

	public Map<String, Object> map() {
		Map<String, Object> m = Maps.of();
		m.putAll(this);
		return m;
	}

	public synchronized Rmap map(Map<String, Object> map) {
		clear();
		putAll(map);
		return this;
	}

	@Override
	public String toString() {
		return "[Table: " + table + ", Key: " + (null == keyField ? "" : (keyField + " => ")) + key + ", Op: " + opname(op) + "] => \n\t"
				+ super.toString();
	}

	// Transfer
	public Rmap skeleton() {
		Rmap r = new Rmap(table(), key).op(op);
		if (null != keyField) r.keyField(keyField);
		return r;
	}

	public Rmap table(Qualifier table) {
		this.table = table;
		return this;
	}

	public Rmap table(Qualifier table, String expr) {
		this.table = table;
		this.tableExpr = expr;
		return this;
	}

	public Rmap op(@Op int op) {
		this.op = op;
		return this;
	}

	public Rmap keyField(String keyField) {
		this.keyField = keyField;
		if (null == keyField) return this;
		Object k = get(keyField);
		if (null != k) key(k);
		return this;
	}

	public @Op int op() {
		return op;
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

	// old format rmap constructor
	@Deprecated
	public Rmap(String table, Map<? extends String, ? extends Object> values) {
		this(new Qualifier(table), values);
	}

	@Deprecated
	public Rmap(String table, Object key, Map<? extends String, ? extends Object> values) {
		this(new Qualifier(table), key, values);
	}

	@Deprecated
	public Rmap(String table, Object key, String firstFieldName, Object... firstFieldValueAndOthers) {
		this(new Qualifier(table), key, firstFieldName, firstFieldValueAndOthers);
	}

	@Deprecated
	public Rmap(String table, Object key) {
		this(new Qualifier(table), key);
	}

	@Deprecated
	public Rmap(String table, Pair<String, Map<String, Object>> keyAndValues) {
		this(new Qualifier(table), keyAndValues);
	}

	@Deprecated
	public Rmap(String table) {
		this(new Qualifier(table));
	}

	@Deprecated
	public Rmap table(String table) {
		return table(new Qualifier(table));
	}

	@Deprecated
	public Rmap table(String table, String expr) {
		return table(new Qualifier(table), expr);
	}
}
