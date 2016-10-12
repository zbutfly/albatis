package net.butfly.albatis.hbase;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseMessage implements Serializable {
	private static final long serialVersionUID = -486156318929790823L;
	private final String table;
	private final byte[] row;
	private final List<Cell> cells = new ArrayList<>();

	public HbaseMessage(String table, byte[] row) {
		super();
		this.table = table;
		this.row = row;
	}

	public HbaseMessage(String table, Result result) {
		super();
		this.table = table;
		this.row = result.getRow();
		for (org.apache.hadoop.hbase.Cell cell : result.rawCells())
			cell(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
	}

	public long size() {
		long s = 0;
		for (Cell c : cells)
			s += c.size();
		return s;
	}

	public String getTable() {
		return table;
	}

	public byte[] getRow() {
		return row;
	}

	public Put put() throws IOException {
		Put put = new Put(row);
		for (Cell c : cells)
			put.add(c.cell(row));
		return put;
	}

	public HbaseMessage cell(String column, byte[] value) {
		cells.add(new Cell(Bytes.toBytes(column), value));
		return this;
	}

	public HbaseMessage cell(String family, String column, byte[] value) {
		cells.add(new Cell(Bytes.toBytes(family), Bytes.toBytes(column), value));
		return this;
	}

	public HbaseMessage cell(String column, byte[] value, long timestamp) {
		cells.add(new Cell(Bytes.toBytes(column), value, timestamp));
		return this;
	}

	public HbaseMessage cell(String family, String column, byte[] value, long timestamp) {
		cells.add(new Cell(Bytes.toBytes(family), Bytes.toBytes(column), value, timestamp));
		return this;
	}

	//
	public HbaseMessage cell(byte[] column, byte[] value) {
		cells.add(new Cell(column, value));
		return this;
	}

	public HbaseMessage cell(byte[] family, byte[] column, byte[] value) {
		cells.add(new Cell(family, column, value));
		return this;
	}

	public HbaseMessage cell(byte[] column, byte[] value, long timestamp) {
		cells.add(new Cell(column, value, timestamp));
		return this;
	}

	public HbaseMessage cell(byte[] family, byte[] column, byte[] value, long timestamp) {
		cells.add(new Cell(family, column, value, timestamp));
		return this;
	}

	public static class Column implements Serializable {
		private static final long serialVersionUID = 1718509410023612905L;
		public static final byte[] DEFAULT_FAMILY = Bytes.toBytes("cf1");
		protected final byte[] family;
		protected final byte[] qualifier;

		private Column(byte[] column) {
			this(DEFAULT_FAMILY, column);
		}

		private Column(byte[] family, byte[] column) {
			this.family = family;
			this.qualifier = column;
		}

		public byte[] getFamily() {
			return family;
		}

		public byte[] getQualifier() {
			return qualifier;
		}

		public long size() {
			return 0;
		}
	}

	public static class Cell extends Column {
		private static final long serialVersionUID = -6245577071706917964L;
		protected final byte[] value;
		protected long timestamp = -1;

		public org.apache.hadoop.hbase.Cell cell(byte[] row) {
			return timestamp < 0 ? new KeyValue(row, family, qualifier, value) : new KeyValue(row, family, qualifier, timestamp, value);
		}

		private Cell(byte[] column, byte[] value) {
			super(column);
			this.value = value;
		}

		private Cell(byte[] family, byte[] column, byte[] value) {
			super(family, column);
			this.value = value;
		}

		private Cell(byte[] column, byte[] value, long timestamp) {
			super(column);
			this.value = value;
			this.timestamp = timestamp;
		}

		private Cell(byte[] family, byte[] column, byte[] value, long timestamp) {
			super(family, column);
			this.value = value;
			this.timestamp = timestamp;
		}

		public byte[] getValue() {
			return value;
		}

		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public long size() {
			return value.length;
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(table).append(":").append(Bytes.toString(row)).append("[").append(cells.size()).append("]");
		sb.append("\n");
		for (Cell c : cells)
			sb.append("\t").append(Bytes.toString(c.family)).append(":").append(Bytes.toString(c.qualifier)).append("@").append(c.timestamp)
					.append("[").append(c.value.length).append("]");
		return sb.toString();
	}
}
