package net.butfly.albatis.hbase;

import static net.butfly.albacore.io.utils.Streams.collect;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.io.Message;
import net.butfly.albacore.io.utils.Streams;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

public final class Hbases extends Utils {
	protected static final Logger logger = Logger.getLogger(Hbases.class);
	public static final String DEFAULT_COL_FAMILY_NAME = "cf1";
	public static final byte[] DEFAULT_COL_FAMILY_VALUE = Bytes.toBytes(DEFAULT_COL_FAMILY_NAME);

	final static ExecutorService ex = Executors.newCachedThreadPool();

	private static Map<String, String> props() {
		Properties p = new Properties();
		try {
			InputStream is = HbaseInput.class.getResourceAsStream("/albatis-hbase.properties");
			if (null != is) p.load(is);
		} catch (IOException e) {}
		return Maps.of(p);
	}

	public static Connection connect() throws IOException {
		return connect(null);
	}

	public static Connection connect(Map<String, String> conf) throws IOException {
		Configuration hconf = HBaseConfiguration.create();
		for (Entry<String, String> c : props().entrySet())
			hconf.set(c.getKey(), c.getValue());
		if (null != conf && !conf.isEmpty()) for (Entry<String, String> c : conf.entrySet())
			hconf.set(c.getKey(), c.getValue());
		return ConnectionFactory.createConnection(hconf, ex);
	}

	public static String colFamily(Cell cell) {
		return Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell));
	}

	public static byte[] toBytes(Result result) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
			if (!result.isEmpty()) result.listCells().forEach(c -> {
				try {
					IOs.writeBytes(baos, cellToBytes(c));
				} catch (Exception e) {}
			});
			return baos.toByteArray();
		}
	}

	public static Result toResult(byte[] bytes) throws IOException {
		List<Cell> cells = new ArrayList<>();
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
			byte[] buf;
			while ((buf = IOs.readBytes(bais)) != null && buf.length > 0)
				cells.add(cellFromBytes(buf));
			return Result.create(cells);
		}
	}

	public static byte[] cellToBytes(Cell cell) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
			return IOs.writeBytes(baos, CellUtil.cloneRow(cell), CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil
					.cloneValue(cell)).toByteArray();
		}
	}

	public static Cell cellFromBytes(byte[] bytes) throws IOException {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
			return new KeyValue(IOs.readBytes(bais), IOs.readBytes(bais), IOs.readBytes(bais), IOs.readBytes(bais));
		}
	}

	public static long totalCellSize(List<Result> rs) {
		return collect(Streams.of(rs), Collectors.summingLong(r -> r.size()));
	}

	public static Map<String, byte[]> map(Stream<Cell> cells) {
		return null == cells ? new ConcurrentHashMap<>()
				: cells.collect(Collectors.toConcurrentMap(c -> Bytes.toString(CellUtil.cloneFamily(c)) + ":" + Bytes.toString(CellUtil
						.cloneQualifier(c)), c -> CellUtil.cloneValue(c)));
	}

	public static Stream<Cell> cells(Message map) {
		byte[] row = Bytes.toBytes(map.key());
		return map.isEmpty() ? Stream.empty() : map.entrySet().stream().map(e -> {
			byte[] v = (byte[]) e.getValue();
			if (null == v || v.length == 0) return null;
			byte[][] fq = Results.parseFQ(e.getKey());
			return CellUtil.createCell(row, fq[0], fq[1], HConstants.LATEST_TIMESTAMP, Type.Put.getCode(), v);
		}).filter(c -> null != c);
	}

	public static interface Results {
		static byte[][] parseFQ(String name) {
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

		static Message result(String table, String row, Stream<Cell> cells) {
			return new Message(table, row, Hbases.map(cells));
		}

		static Message result(String table, Put put) {
			return result(table, Bytes.toString(put.getRow()), put.getFamilyCellMap().values().parallelStream().flatMap(l -> l
					.parallelStream()));
		}

		static Message result(String table, String row, Cell... cells) {
			if (null == cells || cells.length == 0) return new Message(table, row);
			else return result(table, row, Arrays.asList(cells).stream());
		}

		static Message result(String table, String row, List<Cell> cells) {
			if (null == cells || cells.isEmpty()) return new Message(table, row);
			else return result(table, row, cells.stream());
		}

		static Message result(String table, Result result) {
			return result(table, Bytes.toString(result.getRow()), result.listCells());
		}

		static Message hbase(String table, String row, Map<? extends String, ? extends Object> map) {
			return new Message(table, row, map);
		}

		static Put put(Message result) {
			if (result.isEmpty()) return null;
			String row = result.key();
			if (row == null || row.isEmpty()) return null;
			Put put = new Put(Bytes.toBytes(row));
			Hbases.cells(result).forEach(c -> {
				try {
					put.add(c);
				} catch (Exception e) {
					logger.warn("Hbase cell converting failure, ignored and continued, row: " + row + ", cell: " + Bytes.toString(CellUtil
							.cloneFamily(c)) + ":" + Bytes.toString(CellUtil.cloneQualifier(c)), e);
				}
			});
			return put;
		}
	}
}
