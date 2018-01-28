package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue.Supplier;
import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Message;

public final class HbaseInput extends Namedly implements Input<Message> {
	private final long SCAN_BYTES = Props.propL(HbaseInput.class, "scan.bytes", 3145728); // 3M
	private final int SCAN_ROWS = Props.propI(HbaseInput.class, "scan.rows", 1);
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
			else throw new RuntimeException("No table defined for input.");
		}
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
		final ResultScanner scaner;

		public TableScaner(String table) {
			super();
			name = table;
			Scan sc = new Scan();
			try {
				scaner = hconn.table(table).getScanner(Hbases.optimize(sc, batchSize(), SCAN_ROWS, SCAN_BYTES));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public TableScaner(String table, byte[][] startAndEndRow) {
			super();
			name = table;
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

		public TableScaner(String table, Filter[] filters) {
			name = table;
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
			table(t);
	}

	private void table(String table, Supplier<TableScaner> constr) {
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

	public void table(String table, Filter... filter) {
		table(table, () -> new TableScaner(table, filter));
	}

	public void table(String table, byte[]... startAndEndRow) {
		table(table, () -> new TableScaner(table, startAndEndRow));
	}

	public void table(String table) {
		table(table, () -> new TableScaner(table));
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).sizing(Result::getTotalSizeOfCells).<Result> sampling(r -> Bytes.toString(r.getRow()));
	}

	private class TableScaner {
		final String name;
		final ResultScanner scaner;

		public TableScaner(String table) {
			super();
			name = table;
			Scan sc = new Scan();
			try {
				scaner = hconn.table(table).getScanner(Hbases.optimize(sc, batchSize(), SCAN_ROWS, SCAN_BYTES));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public TableScaner(String table, byte[][] startAndEndRow) {
			super();
			name = table;
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

		public TableScaner(String table, Filter[] filters) {
			name = table;
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
			table(t);
	}

	private void table(String table, Supplier<TableScaner> constr) {
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

	public void table(String table, Filter... filter) {
		table(table, () -> new TableScaner(table, filter));
	}

	public void table(String table, byte[]... startAndEndRow) {
		table(table, () -> new TableScaner(table, startAndEndRow));
	}

	public void table(String table) {
		table(table, () -> new TableScaner(table));
	}

	public void tableWithFamily(String table, String... cf) {
		Filter[] fs = filterFamily(cf);
		if (null == fs) table(table);
		else table(table, fs);
	}

	private Filter[] filterFamily(String... cf) {
		if (null == cf || 0 == cf.length) return null;
		return Arrays.stream(cf).map(c -> new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(c)))).toArray(
				i -> new Filter[i]);
	}

	private Filter filterPrefix(List<String> prefixes) {
		if (null == prefixes || prefixes.isEmpty()) return null;
		if (1 == prefixes.size()) return null == prefixes.get(0) ? null : new ColumnPrefixFilter(Bytes.toBytes(prefixes.get(0)));
		byte[][] ps = prefixes.stream().filter(p -> null != p).map(Bytes::toBytes).filter(b -> null != b && b.length > 0).toArray(
				i -> new byte[i][]);
		if (null == ps || ps.length == 0) return null;
		if (ps.length == 1) return new ColumnPrefixFilter(ps[0]);
		else return new MultipleColumnPrefixFilter(ps);
	}

	public void tableWithPrefix(String table, String... prefix) {
		Filter f = filterPrefix(Arrays.asList(prefix));
		if (null == f) table(table);
		else table(table, f);
	}

	public void tableWithFamilAndPrefix(String table, List<String> prefixes, String... cf) {
		Filter pf = filterPrefix(prefixes);
		Filter[] ff = filterFamily(cf);
		List<Filter> filters = null == ff ? Colls.list() : Colls.list(ff);
		if (null != pf) filters.add(pf);
		if (filters.isEmpty()) table(table);
		else table(table, filters.toArray(new Filter[(filters.size())]));
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).sizing(Result::getTotalSizeOfCells).<Result> sampling(r -> Bytes.toString(r.getRow()));
	}

	@Override
	public boolean empty() {
		return scansMap.isEmpty();
	}

	@Override
	public void dequeue(Consumer<Sdream<Message>> using) {
		TableScaner s;
		while (opened() && !empty())
			if (null != (s = scans.poll())) {
				try {
					Result[] results = s.next(batchSize());
					if (null != results) {
						if (results.length > 0) {
							List<Message> ms = Colls.list();
							for (Result r : results)
								if (null != r) ms.add(Hbases.Results.result(s.name, r));
							if (!ms.isEmpty()) {
								using.accept(Sdream.of(ms));
								return;
							}
						} else {// end
							s.close();
							s = null;
						}
					}
				} finally {
					if (null != s) scans.offer(s);
				}
			}
	}
}
