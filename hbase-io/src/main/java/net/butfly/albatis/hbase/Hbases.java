package net.butfly.albatis.hbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
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
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.paral.steam.Sdream;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.parallel.Lambdas;
import net.butfly.albatis.io.Message;

public final class Hbases extends Utils {
	protected static final Logger logger = Logger.getLogger(Hbases.class);
	public static final String DEFAULT_COL_FAMILY_NAME = "cf1";
	public static final byte[] DEFAULT_COL_FAMILY_VALUE = Bytes.toBytes(DEFAULT_COL_FAMILY_NAME);

	final static ExecutorService ex = Executors.newCachedThreadPool();

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
		return Sdream.of(rs).map(Result::size).reduce(Lambdas.sumLong());
	}

	public static Map<String, byte[]> map(Stream<Cell> cells) {
		return null == cells ? Maps.of()
				: cells.collect(Collectors.toConcurrentMap(c -> Bytes.toString(CellUtil.cloneFamily(c)) + ":" + Bytes.toString(CellUtil
						.cloneQualifier(c)), c -> CellUtil.cloneValue(c)));
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

		static Row put(Message m) {
			if (m.isEmpty()) return null;
			String row = m.key();
			if (row == null || row.isEmpty()) return null;
			byte[] rowk = Bytes.toBytes(row);
			switch (m.op()) {
			case INSERT:
			case UPSERT:
				Put put = new Put(rowk);
				Sdream.of(m).map(e -> {
					Object v = e.getValue();
					String k = e.getKey();
					if (!(v instanceof byte[]) || null == ((byte[]) v) || ((byte[]) v).length == 0) return null;
					byte[][] fq = Results.parseFQ(k);
					return CellUtil.createCell(rowk, fq[0], fq[1], HConstants.LATEST_TIMESTAMP, Type.Put.getCode(), (byte[]) v);
				}).nonNull().eachs(c -> {
					try {
						put.add(c);
					} catch (Exception ee) {
						logger.warn("Hbase cell converting failure, ignored and continued, row: " + row + ", cell: " + Bytes.toString(
								CellUtil.cloneFamily(c)) + ":" + Bytes.toString(CellUtil.cloneQualifier(c)), ee);
					}
				});
				return put;
			case INCREASE:
				Increment inc = new Increment(rowk);
				Sdream.of(m).eachs(e -> {
					byte[][] fq = Results.parseFQ(e.getKey());
					Object v = e.getValue();
					inc.addColumn(fq[0], fq[1], null == v ? 1 : ((Long) v).longValue());
				});
				return inc;
			default:
				logger.warn("Op not support: " + m.toString());
				return null;
			}
		}
	}

	private static Map<String, String> props() {
		Properties p = new Properties();
		try {
			InputStream is = HbaseConnection.class.getResourceAsStream("/albatis-hbase.properties");
			if (null != is) p.load(is);
		} catch (IOException e) {}
		return Maps.of(p);
	}

	private static final Method SET_CHACHING;
	private static final Method SET_BATCH;
	private static final Method SET_MAX_RESULT_SIZE;
	static {
		try {
			SET_CHACHING = Scan.class.getMethod("setCaching", int.class);
			SET_BATCH = Scan.class.getMethod("setBatch", int.class);
			SET_MAX_RESULT_SIZE = Scan.class.getMethod("setMaxResultSize", long.class);
		} catch (NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		}
	}

	public static Scan optimize(Scan s, int rowsPerRpc, int colsPerRpc, long maxBytes) {
		// optimize scan for performance, but hbase throw strang exception...
		try {
			s.setCaching((int) rowsPerRpc);// rows per rpc
		} catch (Throwable t) {
			try {
				SET_CHACHING.invoke(s, rowsPerRpc);
			} catch (Throwable tt) {
				logger.warn("Optimize scan Caching fail", tt);
			}
		}
		try {
			s.setBatch(colsPerRpc);// cols per rpc
		} catch (Throwable t) {
			try {
				SET_BATCH.invoke(s, colsPerRpc);// cols per rpc
			} catch (Throwable tt) {
				logger.warn("Optimize scan Batch fail", tt);
			}
		}
		try {
			SET_MAX_RESULT_SIZE.invoke(s, maxBytes);
		} catch (Throwable t) {
			try {
				SET_MAX_RESULT_SIZE.invoke(s, maxBytes);
			} catch (Throwable tt) {
				logger.warn("Optimize scan MaxResultSize fail", tt);
			}
		}
		return s;
	}
}
