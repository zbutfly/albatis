package net.butfly.albatis.hbase;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX;
import static net.butfly.albatis.hbase.Hbases.Filters.and;
import static net.butfly.albatis.hbase.Hbases.Filters.filterFamily;
import static net.butfly.albatis.hbase.Hbases.Filters.filterPrefix;
import static net.butfly.albatis.io.IOProps.prop;
import static net.butfly.albatis.io.IOProps.propB;
import static net.butfly.albatis.io.IOProps.propI;
import static net.butfly.albatis.io.IOProps.propL;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.hbase.HbaseSkip.SkipMode;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;

public class HbaseInput extends Namedly implements Input<Rmap> {
	private static final long serialVersionUID = 6225222417568739808L;
	final static Logger logger = Logger.getLogger(HbaseInput.class);
	static final long SCAN_BYTES = propL(HbaseInput.class, "scan.bytes", 3145728, "Hbase Scan.setMaxResultSize(bytes)."); // 3M
	static final int SCAN_COLS = propI(HbaseInput.class, "scan.cols.per.row", 1, "Hbase Scan.setBatch(cols per rpc).");
	static final boolean SCAN_CACHE_BLOCKS = propB(HbaseInput.class, "scan.cache.blocks", false, "Hbase Scan.setCacheBlocks(false).");
	static final int SCAN_MAX_CELLS_PER_ROW = propI(HbaseInput.class, "scan.max.cells.per.row", 10000,
			"Hbase max cells per row (more will be ignore).");
	static final Pair<SkipMode, String> SCAN_SKIP = SkipMode
			.parse(prop(HbaseInput.class, "scan.skip", null, "Hbase scan skip (for debug or resume): ROWS:n, REGIONS:n, [ROWKEY:]n."));
	final HbaseConnection hconn;
	protected final BlockingQueue<HbaseTableScaner> SCAN_POOL = new LinkedBlockingQueue<>();
	protected final Map<String, List<HbaseTableScaner>> SCAN_REGS = Maps.of();

	public HbaseInput(String name, HbaseConnection conn) {
		super(name);
		hconn = conn;
		closing(this::closeHbase);
	}

	@Override
	public void open() {
		if (SCAN_REGS.isEmpty() && null != hconn.uri().getFile()) table(hconn.uri().getFile());
		Input.super.open();
	}

	private void closeHbase() {
		HbaseTableScaner s;
		while (!SCAN_REGS.isEmpty()) if (null != (s = SCAN_POOL.poll())) try {
			s.close();
		} catch (Exception e) {}
		try {
			hconn.close();
		} catch (Exception e) {}
	}

	@Override
	public void dequeue(Consumer<Sdream<Rmap>> using) {
		HbaseTableScaner s;
		boolean end = false;
		Map<String, Rmap> ms = Maps.of();
		while (!end && opened() && !empty()) if (null != (s = SCAN_POOL.poll())) try {
			Result[] results = s.next(batchSize());
			if (!(end = Colls.empty(results))) {
				Rmap last = lastRmaps.remove(s.table);
				if (null != last) {
					Rmap m = ms.get(last.key());
					if (null != m) m.putAll(last);
					else ms.put((String) last.key(), last);
				}
				for (Result r : results) if (null != r) compute(ms, Hbases.Results.result(s.logicalName, r));
				if (end = scanLast(s, ms)) {
					compute(ms, lastRmaps.remove(s.table));
					s.close();
					s = null;
				}
			}
		} finally {
			if (null != s) SCAN_POOL.offer(s);
		}
		if (!ms.isEmpty()) using.accept(of(ms.values()));
	}

	public void table(String... table) {
		for (String t : table) table(t, t);
	}

	protected void table(String table, String logicalTable, Filter filter) {
		new HbaseTableScaner(this, table, logicalTable, filter);
	}

	public void table(String table, String logicalTable, byte[]... startAndEndRow) {
		new HbaseTableScaner(this, table, logicalTable, null, startAndEndRow);
	}

	public void table(String table, String logicalTable) {
		new HbaseTableScaner(this, table, logicalTable, null);
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
		Filter f = and(filterPrefix(prefixes), filterFamily(cf));

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
		return SCAN_REGS.isEmpty();
	}

	protected final Map<String, Rmap> lastRmaps = Maps.of();

	protected boolean scanLast(HbaseTableScaner s, Map<String, Rmap> ms) {
		Rmap last = null;
		Object rowkey = null;
		Result r;
		try {
			if (null == (r = s.next())) return true;
			Rmap m = Hbases.Results.result(s.logicalName, r);
			if (null == (last = ms.remove(m.key()))) lastRmaps.put(s.table, m); // 1st cell of a diff record
			else {
				last.putAll(m); // same record
				// if (last.size() > MAX_CELLS_PER_ROW && null == rowkey) {
				// rowkey = m.key();
				// logger.warn("Too many (>" + MAX_CELLS_PER_ROW + ") cells in row [" + rowkey + "]" + last.size());
				// ms.put((String) last.key(), last);
				// } else //
				lastRmaps.put(s.table, last);
			}
			return false;
		} finally {
			if (null != last && null != rowkey) //
				logger.warn("Too many cells in row [" + rowkey + "] and finished, [" + last.size() + "] cells found.");
		}
	}

	protected static void compute(Map<String, Rmap> ms, Rmap m) {
		if (!Colls.empty(m)) ms.compute((String) m.key(), (rowkey, existed) -> {
			if (null == existed) return m;
			existed.putAll(m);
			return existed;
		});
	}

	public static void main(String[] args) throws InterruptedException {
		System.err.println(HbaseInput.SCAN_MAX_CELLS_PER_ROW);
		// while (true)
		// Thread.sleep(10000);
	}
}
