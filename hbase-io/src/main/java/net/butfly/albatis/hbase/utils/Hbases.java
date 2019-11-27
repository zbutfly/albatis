package net.butfly.albatis.hbase.utils;

import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF_CH;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX;
import static net.butfly.albatis.io.Rmap.Op.INCREASE;
import static net.butfly.albatis.io.Rmap.Op.INSERT;
import static net.butfly.albatis.io.Rmap.Op.UPSERT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
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
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.QualifierField;
import net.butfly.albatis.hbase.HbaseConnection;
import net.butfly.albatis.io.Rmap;

@SuppressWarnings("deprecation")
public final class Hbases extends Utils {
	protected static final Logger logger = Logger.getLogger(Hbases.class);
	public static final String DEFAULT_COL_FAMILY_NAME = "cf1";
	public static final byte[] DEFAULT_COL_FAMILY_VALUE = Bytes.toBytes(DEFAULT_COL_FAMILY_NAME);

	public final static ExecutorService ex = Executors.newCachedThreadPool();

	public static Connection connect() throws IOException {
		return connect(null);
	}

	public static Connection connect(Map<String, String> conf, InputStream... res) throws IOException {
		Configuration hconf = HBaseConfiguration.create();
		String kerberosPath = conf.remove("kerberos");
		for (Entry<String, String> c : props().entrySet()) hconf.set(c.getKey(), c.getValue());
		if (null != conf && !conf.isEmpty()) for (Entry<String, String> c : conf.entrySet()) hconf.set(c.getKey(), c.getValue());
		if (!Colls.empty(res)) for (InputStream r : res) hconf.addResource(r);
		// 1.hbase.security.authentication = kerberos/normal (albatis-hbase.properties)
		if (User.isHBaseSecurityEnabled(hconf)) Kerberoses.kerberosAuth(hconf, kerberosPath);

		while (true) try {
			return ConnectionFactory.createConnection(hconf, ex);
		} catch (IOException e) {
			throw e;
		} catch (Throwable t) {// org.apache.zookeeper.KeeperException.ConnectionLossException
			throw new IOException(t);
		}
	}

	public static void disconnect(Connection conn) throws IOException {
		if (null != conn) try {
			synchronized (conn) {
				if (!conn.isClosed()) conn.close();
			}
		} catch (IOException e) {
			logger.error("Hbase connection close failure", e);
		}
	}

	public static String colFamily(Cell cell) {
		return Bytes.toString(CellUtil.cloneFamily(cell)) + SPLIT_CF + Bytes.toString(CellUtil.cloneQualifier(cell));
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
			while ((buf = IOs.readBytes(bais)) != null && buf.length > 0) cells.add(cellFromBytes(buf));
			return Result.create(cells);
		}
	}

	public static byte[] cellToBytes(Cell cell) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
			return IOs.writeBytes(baos, CellUtil.cloneRow(cell), CellUtil.cloneFamily(cell), //
					CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell)).toByteArray();
		}
	}

	public static Cell cellFromBytes(byte[] bytes) throws IOException {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
			return new KeyValue(IOs.readBytes(bais), IOs.readBytes(bais), IOs.readBytes(bais), IOs.readBytes(bais));
		}
	}

	public static long totalCellSize(Result... rs) {
		return Arrays.stream(rs).flatMap(r -> r.listCells().stream()).mapToInt(c -> CellUtil.estimatedSerializedSizeOf(c)).sum();
	}

	public static long totalCellSize(List<Result> rs) {
		return rs.stream().flatMap(r -> r.listCells().stream()).mapToInt(c -> CellUtil.estimatedSerializedSizeOf(c)).sum();
	}

	public static Map<String, byte[]> map(Stream<Cell> cells) {
		return null == cells ? Maps.of() : cells.collect(Collectors.toConcurrentMap(c -> {
			String cf = Bytes.toString(CellUtil.cloneFamily(c));
			String[] fqs = Bytes.toString(CellUtil.cloneQualifier(c)).split(SPLIT_PREFIX, 2);
			return cf + SPLIT_CF + (fqs.length == 2 ? fqs[0] + SPLIT_PREFIX + fqs[1] : fqs[0]);
		}, c -> CellUtil.cloneValue(c)));
	}

	public static interface Results {
		static Map<String, byte[]> values(Result r) {
			Map<String, byte[]> m = Maps.of();
			if (null == r) return Maps.of();
			for (Cell c : r.rawCells()) m.put(//
					Bytes.toString(CellUtil.cloneFamily(c)) + SPLIT_CF_CH + Bytes.toString(CellUtil.cloneQualifier(c)), //
					CellUtil.cloneValue(c));
			return m;
		}

		static Rmap result(String table, String row, Stream<Cell> cells) {
			return new Rmap(table, row, Hbases.map(cells));
		}

		static Rmap result(String table, Put put) {
			return result(table, Bytes.toString(put.getRow()), //
					put.getFamilyCellMap().values().parallelStream().flatMap(l -> l.parallelStream()));
		}

		static Rmap result(String table, String row, Cell... cells) {
			if (null == cells || cells.length == 0) return new Rmap(table, row);
			else return result(table, row, Arrays.stream(cells));
		}

		static Rmap result(String table, String row, List<Cell> cells) {
			if (Colls.empty(cells)) return new Rmap(table, row);
			else return result(table, row, cells.stream());
		}

		static Rmap result(String table, Result result) {
			if (null == result) return null;
			else return result(table, Bytes.toString(result.getRow()), result.listCells());
		}

		static Rmap hbase(String table, String row, Map<? extends String, ? extends Object> map) {
			return new Rmap(table, row, map);
		}

		@SuppressWarnings("unchecked")
		@Deprecated
		static Mutation op(Rmap m, Function<Map<String, Object>, byte[]> conv) {
			if (m.isEmpty()) return null;
			Object rowo = m.key();
			if (null == rowo) return null;
			String row = rowo.toString();
			if (row.isEmpty()) return null;
			byte[] rowk = Bytes.toBytes(row);
			int fc = 0;
			switch (m.op()) {
			case INSERT:
			case UPSERT:
				Put put = new Put(rowk);
				for (Entry<String, Object> e : m.entrySet()) {
					byte[] val = null;
					Object v = e.getValue();
					if (v instanceof byte[]) val = (byte[]) v;
					else if (v instanceof CharSequence) {
						String s = ((CharSequence) v).toString();
						if (s.length() > 0) val = Bytes.toBytes(s);
					} else if (v instanceof Map) val = conv.apply((Map<String, Object>) v);
					if (null == val || val.length == 0) return null;
					Cell c = cell(QualifierField.of(m.table(), e.getKey()), rowk, (byte[]) val);
					if (null != c) try {
						put.add(c);
						fc++;
					} catch (Exception ee) {
						logger.warn("Hbase cell converting failure, ignored and continued, row: " + row + ", cell: " //
								+ Bytes.toString(CellUtil.cloneFamily(c)) + SPLIT_CF + Bytes.toString(CellUtil.cloneQualifier(c)), ee);
					}
				}
				return fc == 0 ? null : put;
			case INCREASE:
				Increment inc = new Increment(rowk);
				for (Entry<String, Object> e : m.entrySet()) {
					Object v = e.getValue();
					inc(inc, QualifierField.of(m.table(), e.getKey()), null == v ? 1 : ((Long) v).longValue());
					fc++;
				}
				return inc;
			default:
				logger.warn("Op not support: " + m.toString());
				return null;
			}
		}

		static Cell cell(QualifierField qf, byte[] rowk, byte[] val) {
			return CellUtil.createCell(rowk, Bytes.toBytes(qf.family), //
					Bytes.toBytes((null == qf.prefix ? "" : qf.prefix + SPLIT_PREFIX) + qf.name), //
					HConstants.LATEST_TIMESTAMP, Type.Put.getCode(), val);
		}

		static void inc(Increment inc, QualifierField qf, long v) {
			inc.addColumn(Bytes.toBytes(qf.family), Bytes.toBytes((null == qf.prefix ? "" : qf.prefix + SPLIT_PREFIX) + qf.name), v);
		}

		static Mutation op(Rmap m) {
			if (m.isEmpty()) return null;
			Object rowo = m.key();
			if (null == rowo) return null;
			String row = rowo.toString();
			if (row.isEmpty()) return null;
			byte[] rowk = Bytes.toBytes(row);
			int fc = 0;
			switch (m.op()) {
			case INSERT:
			case UPSERT:
				Put put = new Put(rowk);
				for (Entry<String, Object> e : m.entrySet()) {
					byte[] val = null;
					Object v = e.getValue();
					if (v instanceof byte[]) val = (byte[]) v;
					else if (v instanceof CharSequence) {
						String s = ((CharSequence) v).toString();
						if (s.length() > 0) val = Bytes.toBytes(s);
					} else if (v instanceof Map) {
						logger.warn("Map field [" + e.getKey() + "] not supported, add ?df=format to conv, value lost: \n\t" + v.toString());
						return null;
					}
					if (null == val || val.length == 0) return null;
					Cell c = cell(QualifierField.of(m.table(), e.getKey()), rowk, (byte[]) val);
					if (null != c) try {
						put.add(c);
						fc++;
					} catch (Exception ee) {
						logger.warn("Hbase cell converting failure, ignored and continued, row: " + row + ", cell: " //
								+ Bytes.toString(CellUtil.cloneFamily(c)) + SPLIT_CF + Bytes.toString(CellUtil.cloneQualifier(c)), ee);
					}
				}
				return fc == 0 ? null : put;
			case INCREASE:
				Increment inc = new Increment(rowk);
				for (Entry<String, Object> e : m.entrySet()) {
					inc(inc, QualifierField.of(m.table(), e.getKey()), null == e.getValue() ? 1 : ((Long) e.getValue()).longValue());
					fc++;
				}
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

	public interface Filters {
		static Filter limitCells(Filter f, int rows) {
			return and(f, new ColumnCountGetFilter(rows));
		}

		static Filter or(Filter... fl) {
			List<Filter> l = Colls.list();
			for (Filter f : fl) if (null != f) l.add(f);
			if (l.isEmpty()) return null;
			return l.size() == 1 ? l.get(0) : new FilterList(Operator.MUST_PASS_ONE, l);
		}

		static Filter and(Filter... fl) {
			List<Filter> l = Colls.list();
			for (Filter f : fl) if (null != f) l.add(f);
			if (l.isEmpty()) return null;
			return l.size() == 1 ? l.get(0) : new FilterList(Operator.MUST_PASS_ALL, l);
		}

		static Filter filterFamily(Collection<String> cf) {
			if (Colls.empty(cf)) return null;
			List<Filter> fl = Colls.list();
			for (String c : cf) fl.add(new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(c))));
			return fl.size() == 1 ? fl.get(0) : or(fl.toArray(new Filter[0]));
		}

		static Filter filterPrefix(Collection<String> prefixes) {
			if (Colls.empty(prefixes)) return null;
			if (1 == prefixes.size()) {//
				String p0 = prefixes.iterator().next();
				return null == p0 ? null : new ColumnPrefixFilter(Bytes.toBytes(p0 + SPLIT_PREFIX));
			}
			byte[][] ps = Colls.list(prefixes, p -> Bytes.toBytes(p + SPLIT_PREFIX)).toArray(new byte[0][]);
			if (null == ps || ps.length == 0) return null;
			if (ps.length == 1) return new ColumnPrefixFilter(ps[0]);
			else return new MultipleColumnPrefixFilter(ps);
		}
	}
}
