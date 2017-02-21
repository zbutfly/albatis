package net.butfly.albatis.hbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.Message;
import net.butfly.albacore.io.Streams;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.logger.Logger;

public class HbaseResult extends Message<String, Put, HbaseResult> {
	private static final long serialVersionUID = -486156318929790823L;
	private static final Logger logger = Logger.getLogger(HbaseResult.class);
	public static final String DEFAULT_COL_FAMILY_NAME = "cf1";
	public static final byte[] DEFAULT_COL_FAMILY_VALUE = Bytes.toBytes(DEFAULT_COL_FAMILY_NAME);

	private String table;
	private byte[] row;
	private Result result;

	public HbaseResult(String table, byte[] row, Cell... cells) {
		this(table, row, Arrays.asList(cells));
	}

	public HbaseResult(String table, byte[] row, List<Cell> cells) {
		super();
		this.table = table;
		this.row = row;
		this.result = Result.create(IO.list(Streams.of(cells)));
	}

	public HbaseResult(String table, Result result) {
		super();
		init(table, result);
	}

	private void init(String table, Result result) {
		this.table = table;
		this.row = result.getRow();
		this.result = result;
	}

	public long size() {
		return isEmpty() ? 0 : cells().mapToLong(c -> c.getValueLength()).sum();
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

	@Override
	public Put forWrite() {
		Put put = new Put(row);
		if (!isEmpty()) for (Cell c : result.rawCells())
			try {
				put.add(c);
			} catch (Exception e) {
				logger.warn("Hbase cell converting failure, ignored and continued, row: " + Bytes.toString(row) + ", cell: " + Bytes
						.toString(CellUtil.cloneFamily(c)) + Bytes.toString(CellUtil.cloneQualifier(c)), e);
			}
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
		return new StringBuilder(table).append(":").append(Bytes.toString(row)).append("[").append(size()).append("]").append("\n").append(
				cells().map(c -> new StringBuilder().append("\t").append(CellUtil.cloneFamily(c)).append(":").append(CellUtil
						.cloneQualifier(c)).append("[").append(c.getValueLength()).append("]")).collect(Collectors.joining("\n")))
				.toString();
	}

	public Stream<Cell> cells() {
		return isEmpty() ? Stream.empty() : Stream.of(result.rawCells());
	}

	public Set<String> cols() {
		return isEmpty() ? null : IO.collect(cells().map(Hbases::colFamily), Collectors.toSet());
	}

	public boolean isEmpty() {
		return null == result || result.isEmpty();
	}

	@Override
	public byte[] toBytes() {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
			IOs.writeBytes(baos, table.getBytes());
			IOs.writeBytes(baos, Hbases.toBytes(result));
			return baos.toByteArray();
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public String partition() {
		return table;
	}

	public HbaseResult(byte[] bytes) {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);) {
			init(new String(IOs.readBytes(bais)), Hbases.toResult(IOs.readBytes(bais)));
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public String id() {
		return Bytes.toString(row);
	}
}
