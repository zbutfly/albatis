package net.butfly.albatis.hbase;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.lambda.Consumer;
import net.butfly.albacore.utils.Collections;

public class HbaseResult implements Serializable {
	private static final long serialVersionUID = -486156318929790823L;
	public static final String DEFAULT_COL_FAMILY_NAME = "cf1";
	public static final byte[] DEFAULT_COL_FAMILY_VALUE = Bytes.toBytes(DEFAULT_COL_FAMILY_NAME);

	private final String table;
	private final byte[] row;
	private final Result result;

	// private final Map<String, Cell> cells = new HashMap<>();

	public HbaseResult(String table, byte[] row, Cell... cells) {
		this(table, row, Arrays.asList(cells));
	}

	public HbaseResult(String table, byte[] row, List<Cell> cells) {
		super();
		this.table = table;
		this.row = row;
		this.result = Result.create(Collections.noNull(cells));
	}

	public HbaseResult(String table, Result result) {
		super();
		this.table = table;
		this.row = result.getRow();
		this.result = result;
	}

	public long size() {
		long s = 0;
		for (Cell c : result.rawCells())
			s += c.getValueLength();
		return s;
	}

	public String getTable() {
		return table;
	}

	public byte[] getRow() {
		return row;
	}

	public Result getResult() {
		return result;
	}

	public Put put() throws IOException {
		Put put = new Put(row);
		for (Cell c : result.rawCells())
			put.add(c);
		return put;
	}

	public byte[] get(String col) throws IOException {
		String[] cols = col.split(":");
		switch (cols.length) {
		case 0:
			return null;
		case 1:
			return get(DEFAULT_COL_FAMILY_NAME, col);
		default:
			return get(cols[0], cols[1]);
		}
	}

	public byte[] get(String family, String col) throws IOException {
		return CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes(family), Bytes.toBytes(col)));
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(table).append(":").append(Bytes.toString(row)).append("[").append(size()).append("]");
		sb.append("\n");
		for (Cell c : result.rawCells())
			sb.append("\t").append(CellUtil.cloneFamily(c)).append(":").append(CellUtil.cloneQualifier(c)).append("[").append(c
					.getValueLength()).append("]");
		return sb.toString();
	}

	public void each(Consumer<Cell> consumer) {
		for (Cell c : result.rawCells())
			consumer.accept(c);
	}

	public Set<String> cols() {
		return new HashSet<String>(Collections.transform(Hbases::colFamily, result.rawCells()));
	}
}
