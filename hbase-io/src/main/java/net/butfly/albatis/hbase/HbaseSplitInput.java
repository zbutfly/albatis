package net.butfly.albatis.hbase;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX;
import static net.butfly.albatis.hbase.HbaseFilters.filterFamily;
import static net.butfly.albatis.hbase.HbaseFilters.filterPrefix;
import static net.butfly.albatis.hbase.HbaseFilters.limitCells;
import static net.butfly.albatis.hbase.HbaseFilters.or;
import static net.butfly.albatis.io.IOProps.propB;
import static net.butfly.albatis.io.IOProps.propI;
import static net.butfly.albatis.io.IOProps.propL;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
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
import net.butfly.albacore.io.lambda.Supplier;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;

public class HbaseSplitInput extends Namedly implements Input<Rmap> {
	private static final long serialVersionUID = 6225222417568739808L;
	static final long SCAN_BYTES = propL(HbaseSplitInput.class, "scan.bytes", 3145728, "Hbase Scan.setMaxResultSize(bytes)."); // 3M
	static final int SCAN_COLS = propI(HbaseSplitInput.class, "scan.cols.per.row", 1, "Hbase Scan.setBatch(cols per rpc).");
	static final boolean SCAN_CACHE_BLOCKS = propB(HbaseSplitInput.class, "scan.cache.blocks", false, "Hbase Scan.setCacheBlocks(false).");
	static final int SCAN_MAX_CELLS_PER_ROW = propI(HbaseSplitInput.class, "scan.max.cells.per.row", 10000,
			"Hbase max cells per row (more will be ignore).");
	private final HbaseConnection hconn;
	private final BlockingQueue<TableScaner> scans = new LinkedBlockingQueue<>();

	public HbaseSplitInput(String name, HbaseConnection conn) {
		super(name);
		hconn = conn;
		closing(this::closeHbase);
	}

	@Override
	public void open() {
		if (scans.isEmpty()) {
			if (null != hconn.uri().getFile()) table(hconn.uri().getFile());
			// else throw new RuntimeException("No table defined for input.");
		}
		Input.super.open();
	}

	private void closeHbase() {
		TableScaner s;
		while (null != (s = scans.poll())) s.close();
		try {
			hconn.close();
		} catch (Exception e) {}
	}

	private class TableScaner {
		final String name;
		final String logicalName;
		final ResultScanner scaner;
		final Iterator<Result> it;

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
				sc = sc.setFilter(limitCells(f));
			}
			try {
				scaner = hconn.table(name).getScanner(HbaseFilters.optimize(sc, batchSize(), SCAN_COLS));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			it = scaner.iterator();
		}

		public void close() {
			try {
				scaner.close();
			} catch (Exception e) {}
		}

		public Result next() {
			return it.next();
		}
	}

	private List<TableScaner> regions(String table, String logicalTable, Filter f, byte[]... startAndEndRow) {
		if (null == startAndEndRow || 0 == startAndEndRow.length) // split into regions
			return Colls.list(hconn.ranges(table), range -> new TableScaner(table, logicalTable, f, range.v1(), range.v2()));
		else if (1 == startAndEndRow.length) return Colls.list(new TableScaner(table, logicalTable, f, startAndEndRow[0]));
		else return Colls.list(new TableScaner(table, logicalTable, f, startAndEndRow[0], startAndEndRow[1]));
	}

	public void table(String... table) {
		for (String t : table) table(t, t);
	}

	private void table(String table, String logicalTable, Supplier<List<TableScaner>> constr) {
		for (TableScaner ts : scans) if (table.equals(ts.name)) {
			logger().error("Table [" + table + "] input existed and conflicted, ignore new scan request.");
			return;
		}
		scans.addAll(constr.get());
	}

	private void table(String table, String logicalTable, Filter filter) {
		table(table, logicalTable, () -> regions(table, logicalTable, filter));
	}

	public void table(String table, String logicalTable, byte[]... startAndEndRow) {
		table(table, logicalTable, () -> regions(table, logicalTable, null, startAndEndRow));
	}

	public void table(String table, String logicalTable) {
		table(table, logicalTable, () -> regions(table, logicalTable, null));
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
		return scans.isEmpty();
	}

	private Map<String, Rmap> lastRmaps = Maps.of();

	@Override
	public void dequeue(Consumer<Sdream<Rmap>> using) {
		TableScaner s;
		Map<String, Rmap> ms = Maps.of();
		Result r;
		boolean end = false;
		while (opened() && !empty()) if (null != (s = scans.poll())) {
			try {
				while (!end && ms.size() < batchSize() && !(end = (null == (r = s.next())))) {
					Rmap last = lastRmaps.remove(s.name);
					if (null != last) {
						Rmap m = ms.get(last.key());
						if (null != m) m.putAll(last);
						else ms.put((String) last.key(), last);
					}
					compute(ms, Hbases.Results.result(s.logicalName, r));
					end = scanLast(s, ms);
				}
				if (end) {
					String tn = s.name;
					s.close();
					s = null;
					compute(ms, lastRmaps.remove(tn));
				}
			} finally {
				if (null != s) scans.offer(s);
			}
			if (!ms.isEmpty()) using.accept(of(ms.values()));
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

	public static void main(String[] args) throws InterruptedException {
		System.err.println(HbaseSplitInput.SCAN_MAX_CELLS_PER_ROW);
		// while (true)
		// Thread.sleep(10000);
	}
}
