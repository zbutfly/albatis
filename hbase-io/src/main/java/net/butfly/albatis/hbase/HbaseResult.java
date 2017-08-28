package net.butfly.albatis.hbase;

import static net.butfly.albacore.io.utils.Streams.collect;
import static net.butfly.albacore.io.utils.Streams.list;
import static net.butfly.albacore.io.utils.Streams.of;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.io.Record;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.logger.Logger;

public class HbaseResult extends Record {
	private static final long serialVersionUID = -486156318929790823L;
	private static final Logger logger = Logger.getLogger(HbaseResult.class);
	public static final String DEFAULT_COL_FAMILY_NAME = "cf1";
	public static final byte[] DEFAULT_COL_FAMILY_VALUE = Bytes.toBytes(DEFAULT_COL_FAMILY_NAME);

	private byte[] row;
	// private Result result;

	public HbaseResult(String table, byte[] row, Stream<Cell> cells) {
		super(table, map(cells));
		this.row = row;
	}

	public HbaseResult(String table, Put put) {
		this(table, put.getRow(), put.getFamilyCellMap().values().parallelStream().flatMap(l -> l.parallelStream()));
	}

	public static Map<String, Object> map(Stream<Cell> cells) {
		return null == cells ? new ConcurrentHashMap<>()
				: cells.collect(Collectors.toConcurrentMap(c -> Bytes.toString(CellUtil.cloneFamily(c)) + ":" + Bytes.toString(CellUtil
						.cloneQualifier(c)), c -> CellUtil.cloneValue(c)));
	}

	public HbaseResult(String table, byte[] row, Cell... cells) {
		this(table, row, null == cells ? null : Arrays.asList(cells));
	}

	public HbaseResult(String table, Cell row, Cell... cells) {
		this(table, CellUtil.cloneRow(row), null == cells ? null : Arrays.asList(cells));
	}

	private HbaseResult(String table, byte[] row, List<Cell> cells) {
		this(table, row, null == cells ? null : cells.stream());
	}

	public HbaseResult(String table, Result result) {
		super(table, result.getRow(), result.listCells());
	}

	public static long sizes(Iterable<HbaseResult> results) {
		if (null == results) return 0;
		return of(results).collect(Collectors.summingLong(r -> r.size()));
	}

	public static long cells(Iterable<HbaseResult> results) {
		if (null == results) return 0;
		return of(results).collect(Collectors.summingLong(r -> r.cells().collect(Collectors.counting())));
	}

	public String getTable() {
		return table;
	}

	public byte[] getRow() {
		return row;
	}

	public Result getResult() {
		Stream<Cell> cells = entrySet().stream().map(e -> {
			if (e.getValue() == null) return null;
			Class<? extends Object> c = e.getValue().getClass();
			byte[] v;
			if (c.isArray() && byte.class.equals(c.getComponentType())) v = (byte[]) e.getValue();
			else if (CharSequence.class.isAssignableFrom(c)) v = Bytes.toBytes(e.getValue().toString());
			else return null;// XXX
			String[] fq = e.getKey().split(":", 2);
			if (fq.length == 1) return CellUtil.createCell(row, DEFAULT_COL_FAMILY_VALUE, Bytes.toBytes(fq[0]), System.currentTimeMillis(),
					KeyValue.Type.Put, v);
			if (fq.length == 2) return CellUtil.createCell(row, Bytes.toBytes(fq[0]), Bytes.toBytes(fq[1]), System.currentTimeMillis(),
					KeyValue.Type.Put, v);
			return null;
		});
		return Result.create();
	}

	public Put forWrite() {
		if (isEmpty()) return null;
		if (row == null || row.length == 0) row = result.getRow();
		if (row == null || row.length == 0) return null;
		Put put = new Put(row);
		for (Cell c : result.rawCells())
			try {
				put.add(c);
			} catch (Exception e) {
				logger.warn("Hbase cell converting failure, ignored and continued, row: " + Bytes.toString(row) + ", cell: " + Bytes
						.toString(CellUtil.cloneFamily(c)) + Bytes.toString(CellUtil.cloneQualifier(c)), e);
			}
		return put;
	}

	public byte[] get(String col) throws IOException {
		String[] cols = col.split(":", 2);
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
		return new StringBuilder(table).append(":").append(Bytes.toString(row)).append("[").append(size()).append("]").append("\n").append(
				cells().map(c -> new StringBuilder().append("\t").append(CellUtil.cloneFamily(c)).append(":").append(CellUtil
						.cloneQualifier(c)).append("[").append(c.getValueLength()).append("]")).collect(Collectors.joining("\n")))
				.toString();
	}

	public Stream<Cell> cells() {
		return isEmpty() ? Stream.empty() : Stream.of(result.rawCells());
	}

	public Set<String> cols() {
		return isEmpty() ? null : collect(cells().map(Hbases::colFamily), Collectors.toSet());
	}

	public HbaseResult(byte[] bytes) {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);) {
			init(new String(IOs.readBytes(bais)), Hbases.toResult(IOs.readBytes(bais)));
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

}
