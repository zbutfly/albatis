package net.butfly.albatis.hbase;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
		return isEmpty() ? 0 : result.listCells().parallelStream().collect(Collectors.summarizingInt(c -> c.getValueLength())).getSum();
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

	public Put put() {
		Put put = new Put(row);
		each(c -> {
			try {
				put.add(c);
			} catch (Exception e) {}
		});
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
		each(c -> sb.append("\t").append(CellUtil.cloneFamily(c)).append(":").append(CellUtil.cloneQualifier(c)).append("[").append(c
				.getValueLength()).append("]"));
		return sb.toString();
	}

	public void each(Consumer<Cell> consumer) {
		if (!isEmpty()) result.listCells().parallelStream().forEach(consumer);
	}

	public Set<String> cols() {
		return isEmpty() ? null : new HashSet<String>(Collections.map(result.listCells(), Hbases::colFamily));
	}

	public boolean isEmpty() {
		return null == result || result.isEmpty();
	}
}
