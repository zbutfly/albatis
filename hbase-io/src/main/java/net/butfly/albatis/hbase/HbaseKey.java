package net.butfly.albatis.hbase;

import static net.butfly.albacore.utils.Texts.eq;

public class HbaseKey {

	public String table;
	public String rowKey;
	public String key;

	public HbaseKey(String table, String rowKey) {
		this.table = table;
		this.rowKey = rowKey;
		buildKey();
	}

	public void buildKey() {
		key = table + rowKey;
	}

	@Override
	public String toString() {
		return table + rowKey;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (null == obj || !(obj instanceof HbaseKey))
			return false;
		HbaseKey q = (HbaseKey) obj;
		return eq(table, q.table) && eq(rowKey, q.rowKey) && eq(key, q.key);
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}
}
