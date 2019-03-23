package net.butfly.albatis.hbase;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX;
import static net.butfly.albatis.hbase.HbaseFilters.filterFamily;
import static net.butfly.albatis.hbase.HbaseFilters.filterPrefix;
import static net.butfly.albatis.hbase.HbaseFilters.or;
import static net.butfly.albatis.io.IOProps.propB;
import static net.butfly.albatis.io.IOProps.propI;
import static net.butfly.albatis.io.IOProps.propL;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.io.lambda.Supplier;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;

public class HbaseInput2 extends Namedly implements Input<Rmap> {
	private static final long serialVersionUID = 6225222417568739808L;
	static final long SCAN_BYTES = propL(HbaseInput.class, "scan.bytes", 3145728, "Hbase Scan.setMaxResultSize(bytes)."); // 3M
	static final int SCAN_COLS = propI(HbaseInput.class, "scan.cols.per.row", 1, "Hbase Scan.setBatch(cols per rpc).");
	static final boolean SCAN_CACHE_BLOCKS = propB(HbaseInput.class, "scan.cache.blocks", false, "Hbase Scan.setCacheBlocks(false).");
	static final int SCAN_MAX_CELLS_PER_ROW = propI(HbaseInput.class, "scan.max.cells.per.row", 10000,
			"Hbase max cells per row (more will be ignore).");
	private final HbaseConnection hconn;
	private final BlockingQueue<TableScaner> scans = new LinkedBlockingQueue<>();
	private final Map<String, BlockingQueue<TableScaner>> scansMap = Maps.of();

	public HbaseInput2(String name, HbaseConnection conn) {
		super(name);
		hconn = conn;
		closing(this::closeHbase);
	}

	@Override
	public void open() {
		if (scansMap.isEmpty()) {
			if (null != hconn.uri().getFile()) table(hconn.uri().getFile());
			// else throw new RuntimeException("No table defined for input.");
		}
		Input.super.open();
	}

	private void closeHbase() {
		TableScaner s;
		while (!scansMap.isEmpty()) if (null != (s = scans.poll())) s.close();
		try {
			hconn.close();
		} catch (Exception e) {}
	}

	private class TableScaner {
		final String name;
		final String logicalName;
		final ResultScanner scaner;

		public TableScaner(String table, String logicalTable, Filter f, byte[]... startAndEndRow) {
			super();
			name = table;
			this.logicalName = logicalTable;

			Scan sc;
			if (null == startAndEndRow || 0 == startAndEndRow.length) sc = new Scan();
			else if (1 == startAndEndRow.length) sc = new Scan(startAndEndRow[0]);
			else sc = new Scan(startAndEndRow[0], startAndEndRow[1]);

			if (null != f) {
				logger().debug(name() + " filtered: " + f.toString());
				sc = sc.setFilter(f);
			}
			try {
				scaner = hconn.table(name).getScanner(HbaseFilters.optimize(sc, batchSize(), SCAN_COLS));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public void close() {
			try {
				scaner.close();
			} catch (Exception e) {} finally {
				scansMap.remove(name);
			}
		}

		public Result[] next(int batchSize) {
			try {
				return scaner.next(batchSize);
			} catch (IOException e) {
				return null;
			}
		}

		public Result next() {
			try {
				return scaner.next();
			} catch (IOException e) {
				return null;
			}
		}
	}

	public void table(String... table) {
		for (String t : table) table(t, t);
	}

	private void open(String table, String logicalTable, Function<byte[][], TableScaner> constr) {
		scansMap.compute(table, (t, existed) -> {
			if (null != existed) {
				logger().error("Table [" + table + "] input existed and conflicted, ignore new scan request.");
				return existed;
			}
			existed = new LinkedBlockingQueue<>();
			for (Pair<byte[], byte[]> range : hconn.ranges(table)) {
				TableScaner s = constr.apply(new byte[][] { range.v1(), range.v2() });
				scans.offer(s);
				existed.offer(s);
			}
			return existed;
		});
	}

	private void open1(String table, String logicalTable, Supplier<TableScaner> constr) {
		scansMap.compute(table, (t, existed) -> {
			if (null != existed) {
				logger().error("Table [" + table + "] input existed and conflicted, ignore new scan request.");
				return existed;
			}
			existed = new LinkedBlockingQueue<>();
			TableScaner s = constr.get();
			scans.offer(s);
			existed.offer(s);
			return existed;
		});
	}

	private void table(String table, String logicalTable, Filter filter) {
		open(table, logicalTable, range -> new TableScaner(table, logicalTable, filter, range));
	}

	public void table(String table, String logicalTable, byte[]... startAndEndRow) {
		open1(table, logicalTable, () -> new TableScaner(table, logicalTable, null, startAndEndRow));
	}

	public void table(String table, String logicalTable) {
		open(table, logicalTable, range -> new TableScaner(table, logicalTable, null, range));
	}

	public void tableWithFamily(String table, String... cf) {
		Filter f = filterFamily(cf);
		if (null == f) table(table);
		else table(table, cf.length > 0 ? table : (table + SPLIT_CF + cf[0]), f);
	}

	public void tableWithPrefix(String table, String... prefix) {
		Filter f = filterPrefix(Arrays.asList(prefix));
		if (null == f) table(table);
		else table(table, prefix.length > 0 ? table : (table + SPLIT_PREFIX + prefix[0]), f);
	}

	public void tableWithFamilAndPrefix(String table, List<String> prefixes, String... cf) {
		Filter f = or(filterPrefix(prefixes), filterFamily(cf));

		String logicalTable = table;
		if (cf.length > 0) {
			// if (cf.length > 1) throw new RuntimeException("Now only supports one cf");
			logicalTable += SPLIT_CF + cf[0];
		}
		if (prefixes.size() > 0) {
			// if (prefixes.size() > 1) throw new RuntimeException("Now only supports one prefix");
			logicalTable += SPLIT_PREFIX + prefixes.get(0);
		}

		if (null == f) table(table);
		else table(table, logicalTable, f);
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).sizing(Result::getTotalSizeOfCells).<Result>sampling(r -> Bytes.toString(r.getRow()));
	}

	@Override
	public boolean empty() {
		return scansMap.isEmpty();
	}

	private Map<String, Rmap> lastRmaps = Maps.of();

	@Override
	public void dequeue(Consumer<Sdream<Rmap>> using) {
		TableScaner s;
		while (opened() && !empty()) if (null != (s = scans.poll())) {
			try {
				Result[] results = s.next(batchSize());
				Map<String, Rmap> ms = Maps.of();
				boolean end = Colls.empty(results);
				if (!end) {
					Rmap last = lastRmaps.remove(s.name);
					if (null != last) {
						Rmap m = ms.get(last.key());
						if (null != m) m.putAll(last);
						else ms.put((String) last.key(), last);
					}
					for (Result r : results) if (null != r) compute(ms, Hbases.Results.result(s.logicalName, r));
					end = scanLast(s, ms);
					if (end) {
						String tn = s.name;
						s.close();
						s = null;
						compute(ms, lastRmaps.remove(tn));
					}
					if (!ms.isEmpty()) using.accept(of(ms.values()));
				}
			} finally {
				if (null != s) scans.offer(s);
			}
		}
	}

	private boolean scanLast(TableScaner s, Map<String, Rmap> ms) {
		Rmap last = null;
		Object rowkey = null;
		Result r;
		try {
			if (null == (r = s.next())) return true;
			Rmap m = Hbases.Results.result(s.logicalName, r);
			if (null == (last = ms.remove(m.key()))) lastRmaps.put(s.name, m); // 1st cell of a diff record
			else {
				last.putAll(m); // same record
				// if (last.size() > MAX_CELLS_PER_ROW && null == rowkey) {
				// rowkey = m.key();
				// logger().warn("Too many (>" + MAX_CELLS_PER_ROW + ") cells in row [" + rowkey + "]" + last.size());
				// ms.put((String) last.key(), last);
				// } else //
				lastRmaps.put(s.name, last);
			}
			return false;
		} finally {
			if (null != last && null != rowkey) //
				logger().warn("Too many cells in row [" + rowkey + "] and finished, [" + last.size() + "] cells found.");
		}
	}

	private void compute(Map<String, Rmap> ms, Rmap m) {
		if (!Colls.empty(m)) ms.compute((String) m.key(), (rowkey, existed) -> {
			if (null == existed) return m;
			existed.putAll(m);
			return existed;
		});
	}
}
