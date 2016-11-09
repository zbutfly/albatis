package net.butfly.albatis.hbase;

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import net.butfly.albacore.utils.Objects;

public final class HbaseResultMap implements Map<String, byte[]>, Serializable {
	private static final long serialVersionUID = -8275186612187030057L;
	private final Result result;
	private final byte[] dcf;

	public HbaseResultMap(Result result) {
		this(result, HbaseResult.DEFAULT_COL_FAMILY_VALUE);
	}

	public HbaseResultMap(Result result, byte[] defaultColumnFamily) {
		this.result = result;
		this.dcf = defaultColumnFamily;
	}

	@Override
	public int size() {
		return result.rawCells().length;
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean containsKey(Object key) {
		Objects.noneNull(key);
		return getCell(key.toString()) != null;
	}

	public boolean containsKey(String key) {
		Objects.noneNull(key);
		return getCell(key) != null;
	}

	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] get(Object key) {
		Objects.noneNull(key);
		return CellUtil.cloneValue(getCell(key.toString()));
	}

	@Override
	public ImmutableSet<String> keySet() {
		ImmutableSet.Builder<String> b = ImmutableSet.builder();
		for (Cell c : result.rawCells())
			b.add(Bytes.toString(CellUtil.cloneQualifier(c)), fullKey(c));
		return b.build();
	}

	@Override
	public ImmutableCollection<byte[]> values() {
		ImmutableList.Builder<byte[]> b = ImmutableList.builder();
		for (Cell c : result.rawCells())
			b.add(CellUtil.cloneValue(c));
		return b.build();
	}

	@Override
	public ImmutableSet<Entry<String, byte[]>> entrySet() {
		ImmutableSet.Builder<Entry<String, byte[]>> b = ImmutableSet.builder();
		for (Cell c : result.rawCells())
			b.add(new CellEntry(c));
		return b.build();
	}

	private Cell getCell(String key) {
		String[] ks = key.toString().split(":");
		byte[] q, cf;
		if (ks.length > 1) {
			cf = Bytes.toBytes(ks[0]);
			q = Bytes.toBytes(ks[1]);
		} else {
			cf = dcf;
			q = Bytes.toBytes(ks[0]);
		}
		return result.getColumnLatestCell(cf, q);
	}

	private class CellEntry implements Entry<String, byte[]>, Serializable {
		private static final long serialVersionUID = -2745053103104898708L;
		private final Cell cell;

		public CellEntry(Cell cell) {
			super();
			this.cell = cell;
		}

		@Override
		public String getKey() {
			return fullKey(cell);
		}

		@Override
		public byte[] getValue() {
			return CellUtil.cloneValue(cell);
		}

		@Override
		public byte[] setValue(byte[] value) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean equals(Object object) {
			if (object instanceof Entry) {
				Entry<?, ?> that = (Entry<?, ?>) object;
				return getKey().equals(that.getKey()) && getValue().equals(that.getValue());
			}
			return false;
		}

		@Override
		public int hashCode() {
			String k = getKey();
			byte[] v = getValue();
			return ((k == null) ? 0 : k.hashCode()) ^ ((v == null) ? 0 : v.hashCode());
		}

		/**
		 * Returns a string representation of the form {@code {key}={value}}.
		 */
		@Override
		public String toString() {
			return getKey() + "=(byte[" + getValue().length + "])";
		}
	}

	@Override
	public byte[] put(String key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] remove(Object key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void putAll(Map<? extends String, ? extends byte[]> m) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	private static String fullKey(Cell c) {
		return Bytes.toString(CellUtil.cloneFamily(c)) + ":" + Bytes.toString(CellUtil.cloneQualifier(c));
	}
}
