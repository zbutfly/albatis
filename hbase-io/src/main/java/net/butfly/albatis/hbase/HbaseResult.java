package net.butfly.albatis.hbase;

import static net.butfly.albacore.io.utils.Streams.collect;
import static net.butfly.albacore.io.utils.Streams.of;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.io.Message;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.logger.Logger;

public class HbaseResult extends Message {
	private static final long serialVersionUID = -486156318929790823L;
	private static final Logger logger = Logger.getLogger(HbaseResult.class);
	public static final String DEFAULT_COL_FAMILY_NAME = "cf1";
	public static final byte[] DEFAULT_COL_FAMILY_VALUE = Bytes.toBytes(DEFAULT_COL_FAMILY_NAME);

	private final byte[] row;

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
		this(table, result.getRow(), result.listCells().stream());
	}

	public HbaseResult(String table, byte[] row, Map<? extends String, ? extends Object> map) {
		super(table, map);
		this.row = row;
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
		return Result.create(cells().collect(Collectors.toList()));
	}

	public Put forWrite() {
		if (isEmpty()) return null;
		if (row == null || row.length == 0) return null;
		Put put = new Put(row);
		cells().forEach(c -> {
			try {
				put.add(c);
			} catch (Exception e) {
				logger.warn("Hbase cell converting failure, ignored and continued, row: " + Bytes.toString(row) + ", cell: " + Bytes
						.toString(CellUtil.cloneFamily(c)) + Bytes.toString(CellUtil.cloneQualifier(c)), e);
			}
		});
		return put;
	}

	public byte[] get(String family, String col) throws IOException {
		return cast(get(family + ":" + col));
	}

	@Override
	public String toString() {
		return new StringBuilder(table).append(":").append(Bytes.toString(row)).append("[").append(size()).append("]").append("\n").append(
				cells().map(c -> new StringBuilder().append("\t").append(CellUtil.cloneFamily(c)).append(":").append(CellUtil
						.cloneQualifier(c)).append("[").append(c.getValueLength()).append("]")).collect(Collectors.joining("\n")))
				.toString();
	}

	private byte[] cast(Object src) {
		if (src == null) return null;
		Class<? extends Object> c = src.getClass();
		if (c.isArray() && byte.class.equals(c.getComponentType())) return (byte[]) src;
		else if (CharSequence.class.isAssignableFrom(c)) return Bytes.toBytes(src.toString());
		else return null;// XXX
	}

	private byte[][] parseFQ(String name) {
		String[] fq = name.split(":", 2);
		byte[] f, q;
		if (fq.length == 1) {
			f = DEFAULT_COL_FAMILY_VALUE;
			q = Bytes.toBytes(fq[0]);
		} else {
			f = Bytes.toBytes(fq[0]);
			q = Bytes.toBytes(fq[1]);
		}
		return new byte[][] { f, q };
	}

	public Stream<Cell> cells() {
		return isEmpty() ? Stream.empty() : entrySet().stream().map(e -> {
			byte[] v = cast(e.getValue());
			if (null == v || v.length == 0) return null;
			byte[][] fq = parseFQ(e.getKey());
			return CellUtil.createCell(row, fq[0], fq[1], HConstants.LATEST_TIMESTAMP, Type.Put.getCode(), v);
		}).filter(c -> null != c);
	}

	public Set<String> cols() {
		return isEmpty() ? null : collect(cells().map(Hbases::colFamily), Collectors.toSet());
	}

	@Override
	protected void write(OutputStream os) throws IOException {
		super.write(os);
		IOs.writeBytes(os, row);
	}

	public static HbaseResult fromBytes(byte[] b) {
		if (null == b) throw new IllegalArgumentException();
		try (ByteArrayInputStream bais = new ByteArrayInputStream(b)) {
			Message base = Message.fromBytes(bais);
			byte[] row = IOs.readBytes(bais);
			return new HbaseResult(base.table(), row, base);
		} catch (IOException e) {
			return null;
		}
	}
}
