package net.butfly.albatis.hbase;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albacore.paral.Sdream.of1;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX;
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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
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

public class HbaseInput extends Namedly implements Input<Rmap> {
	private static final long serialVersionUID = 6225222417568739808L;
	private final long SCAN_BYTES = propL(HbaseInput.class, "scan.bytes", 3145728); // 3M
	private final int SCAN_ROWS = propI(HbaseInput.class, "scan.rows", 1);
	private final HbaseConnection hconn;
	private final BlockingQueue<TableScaner> scans = new LinkedBlockingQueue<>();
	private final Map<String, TableScaner> scansMap = Maps.of();

	public HbaseInput(String name, HbaseConnection conn) {
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
		while (!scansMap.isEmpty())
			if (null != (s = scans.poll())) s.close();
		try {
			hconn.close();
		} catch (Exception e) {}
	}

	private class TableScaner {
		final String name;
		final String logicalName;
		final ResultScanner scaner;

		public TableScaner(String table, String logicalTable) {
			super();
			name = table;
			this.logicalName = logicalTable;
			Scan sc = new Scan();
			try {
				scaner = hconn.table(table).getScanner(Hbases.optimize(sc, batchSize(), SCAN_ROWS, SCAN_BYTES));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public TableScaner(String table, String logicalTable, byte[]... startAndEndRow) {
			super();
			name = table;
			this.logicalName = logicalTable;
			Scan sc;
			if (null == startAndEndRow) sc = new Scan();
			else switch (startAndEndRow.length) {
			case 0:
				sc = new Scan();
				break;
			case 1:
				sc = new Scan(startAndEndRow[0]);
				break;
			default:
				sc = new Scan(startAndEndRow[0], startAndEndRow[1]);
				break;
			}
			try {
				scaner = hconn.table(name).getScanner(Hbases.optimize(sc, batchSize(), SCAN_ROWS, SCAN_BYTES));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public TableScaner(String table, String logicalTable, Filter[] filters) {
			name = table;
			this.logicalName = logicalTable;
			Scan sc = new Scan();
			if (null != filters && filters.length > 0) {
				Filter filter = filters.length == 1 ? filters[0] : new FilterList(filters);
				logger().debug(name() + " filtered: " + filter.toString());
				sc = sc.setFilter(filter);
			}
			try {
				scaner = hconn.table(name).getScanner(Hbases.optimize(sc, batchSize(), SCAN_ROWS, SCAN_BYTES));
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
	}

	public void table(String... table) {
		for (String t : table)
			table(t, t);
	}

	private void table(String table, String logicalTable, Supplier<TableScaner> constr) {
		scansMap.compute(table, (t, existed) -> {
			if (null != existed) {
				logger().error("Table [" + table + "] input existed and conflicted, ignore new scan request.");
				return existed;
			}
			TableScaner s = constr.get();
			scans.offer(s);
			return s;
		});
	}

	public void table(String table, String logicalTable, Filter... filter) {
		table(table, logicalTable, () -> new TableScaner(table, logicalTable, filter));
	}

	public void table(String table, String logicalTable, byte[]... startAndEndRow) {
		table(table, logicalTable, () -> new TableScaner(table, logicalTable, startAndEndRow));
	}

	public void table(String table, String logicalTable) {
		table(table, logicalTable, () -> new TableScaner(table, logicalTable));
	}

	public void tableWithFamily(String table, String... cf) {
		Filter fs = filterFamily(cf);
		if (null == fs) table(table);
		else table(table, cf.length > 0 ? table : (table + SPLIT_CF + cf[0]), fs);
	}

	private Filter filterFamily(String... cf) {
		if (null == cf || 0 == cf.length) return null;
		if (cf.length == 1) return new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(cf[0])));
		return new FilterList(Operator.MUST_PASS_ONE, //
				Colls.list(c -> new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(c))), cf));
	}

	private Filter filterPrefix(List<String> prefixes) {
		if (Colls.empty(prefixes)) return null;
		if (1 == prefixes.size()) return null == prefixes.get(0) ? null
				: new ColumnPrefixFilter(Bytes.toBytes(prefixes.get(0) + SPLIT_PREFIX));
		byte[][] ps = Colls.list(prefixes, p -> Bytes.toBytes(p + SPLIT_PREFIX)).toArray(new byte[0][]);
		if (null == ps || ps.length == 0) return null;
		if (ps.length == 1) return new ColumnPrefixFilter(ps[0]);
		else return new MultipleColumnPrefixFilter(ps);
	}

	public void tableWithPrefix(String table, String... prefix) {
		Filter f = filterPrefix(Arrays.asList(prefix));
		if (null == f) table(table);
		else table(table, prefix.length > 0 ? table : (table + SPLIT_PREFIX + prefix[0]), f);
	}

	public void tableWithFamilAndPrefix(String table, List<String> prefixes, String... cf) {
		Filter prefixFilter = filterPrefix(prefixes);
		Filter cfFilter = filterFamily(cf);
		Filter f;
		if (null == prefixFilter) f = cfFilter;
		else if (null == cfFilter) f = prefixFilter;
		else f = new FilterList(Operator.MUST_PASS_ALL, cfFilter, prefixFilter);

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
		return new Statistic(this).sizing(Result::getTotalSizeOfCells).<Result> sampling(r -> Bytes.toString(r.getRow()));
	}

	@Override
	public boolean empty() {
		return scansMap.isEmpty();
	}

	private Map<String, Rmap> lastRmaps = Maps.of();

	@Override
	public void dequeue(Consumer<Sdream<Rmap>> using) {
		TableScaner s;
		while (opened() && !empty())
			if (null != (s = scans.poll())) {
				try {
					Result[] results = s.next(batchSize());
					boolean end = null == results || results.length == 0;
					if (!end) {
						Map<String, Rmap> ms = Maps.of();
						Rmap m = lastRmaps.remove(s.name);
						if (null != m) ms.put((String) m.key(), m);
						for (Result r : results)
							if (null != r) {
								Rmap mm = Hbases.Results.result(s.logicalName, r);
								ms.compute((String) mm.key(), (rowkey, existed) -> {
									if (null == existed) return mm;
									existed.putAll(mm);
									return existed;
								});
							}
						Rmap last;
						while (null != (results = s.next(1)) && results.length > 0) {
							m = Hbases.Results.result(s.logicalName, results[0]);
							if (null != (last = ms.get(m.key()))) last.putAll(m);
							else {
								lastRmaps.put(s.name, m);
								using.accept(of(ms.values()));
								return;
							}
						}
						end = true;
					}
					if (end) {// end
						Rmap m = lastRmaps.remove(s.name);
						s.close();
						s = null;
						if (null != m) using.accept(of1(m));
					}
				} finally {
					if (null != s) scans.offer(s);
				}
			}
	}
}
