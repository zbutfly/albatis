package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.utils.Streams;
import net.butfly.albacore.utils.collection.Maps;

public final class HbaseInput extends Namedly implements Input<HbaseResult> {
	private static final int MAX_RETRIES = 5;
	private final Connection hconn;
	private final String tname;
	private final Table htable;

	private ResultScanner scaner = null;
	private ReentrantReadWriteLock scanerLock;
	private AtomicBoolean ended;

	public HbaseInput(String name, final String table, byte[] startRow, byte[] stopRow) throws IOException {
		super(name);
		hconn = Hbases.connect(Maps.of(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, Integer.toString(Integer.MAX_VALUE)));
		tname = table;
		htable = hconn.getTable(TableName.valueOf(tname), Hbases.ex);

		Scan scan = new Scan(startRow, stopRow);
		scaner = htable.getScanner(scan);
		scanerLock = new ReentrantReadWriteLock();
		ended = new AtomicBoolean(false);

		closing(this::closeHbase);
		open();
	}

	public HbaseInput(String name, final String table, final Filter... filters) throws IOException {
		super(name);
		hconn = Hbases.connect(Maps.of(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, Integer.toString(Integer.MAX_VALUE)));
		tname = table;
		htable = hconn.getTable(TableName.valueOf(tname), Hbases.ex);

		Scan scan = new Scan();
		if (null != filters && filters.length > 0) {
			Filter filter = filters.length == 1 ? filters[0] : new FilterList(filters);
			logger().debug(name() + " filtered: " + filter.toString());
			scan = scan.setFilter(filter);
		}
		scaner = htable.getScanner(scan);
		scanerLock = new ReentrantReadWriteLock();
		ended = new AtomicBoolean(false);

		closing(this::closeHbase);
		open();
	}

	private void closeHbase() {
		try {
			if (null != scaner) scaner.close();
		} catch (Exception e) {}
		try {
			htable.close();
		} catch (Exception e) {}
		try {
			hconn.close();
		} catch (Exception e) {}
		Table t;
		while ((t = tables.poll()) != null)
			try {
				t.close();
			} catch (Exception e) {}
	}

	@Override
	public boolean empty() {
		return ended == null || ended.get();
	}

	@Override
	public long dequeue(Function<Stream<HbaseResult>, Long> using, long batchSize) {
		if (!ended.get() && scanerLock.writeLock().tryLock()) {
			Result[] rs = null;
			try {
				rs = scaner.next((int) batchSize);
			} catch (Exception ex) {
				logger().warn("Hbase failure", ex);
			} finally {
				scanerLock.writeLock().unlock();
			}
			if (null != rs) {
				ended.set(rs.length == 0);
				if (rs.length > 0) return using.apply(Stream.of(rs).map(r -> new HbaseResult(tname, r)));
			}
		}
		return 0;
	}

	public List<HbaseResult> get(List<Get> gets) {
		if (gets == null || gets.isEmpty()) return new ArrayList<>();
		return gets.size() == 1 ? Arrays.asList(get(gets.get(0))) : table(tname, t -> {
			try {
				return IO.list(Arrays.asList(t.get(gets)), r -> new HbaseResult(tname, r));
			} catch (Exception ex) {
				return IO.list(Streams.of(gets, false).map(this::get));
			}
		});
	}

	public HbaseResult get(Get get) {
		return table(tname, t -> {
			try {
				return new HbaseResult(tname, t.get(get));
			} catch (Exception ex) {
				return new HbaseResult(tname, getByScan(get));
			}
		});
	}

	private Result getByScan(Get get) {
		String row = Bytes.toString(get.getRow());
		return table(tname, t -> {
			Result r;
			int retry = 0;
			long now = System.currentTimeMillis();
			do {
				Scan s = new Scan().setRowPrefixFilter(get.getRow());
				if (get.hasFamilies()) for (byte[] cf : get.familySet())
					s.addFamily(cf);
				Filter f = get.getFilter();
				if (null != f) s.setFilter(f);
				ResultScanner sc;
				try {
					sc = t.getScanner(s);
				} catch (IOException ex) {
					logger().error("Hbase get(scan) failed on retry #" + retry + ": [" + row + "] in [" + (System.currentTimeMillis() - now)
							+ " ms], error:\n\t" + ex.getMessage());
					return null;
				}
				try {
					r = sc.next();
				} catch (IOException ex) {
					if (doNotRetry(ex)) {
						logger().error("Hbase get(scan) failed on retry #" + retry + ": [" + row + "] in [" + (System.currentTimeMillis()
								- now) + " ms], error:\n\t" + ex.getMessage());
						return null;
					} else r = null;
				} finally {
					sc.close();
				}
			} while (null == r && retry++ < MAX_RETRIES && opened());
			logger().debug("Hbase get(scan) on [" + row + "] with [" + retry + "] retries");
			return r;
		});
	}

	private boolean doNotRetry(Throwable th) {
		while (!(th instanceof RemoteWithExtrasException) && th.getCause() != null && th.getCause() != th)
			th = th.getCause();
		if (th instanceof RemoteWithExtrasException) return ((RemoteWithExtrasException) th).isDoNotRetry();
		else return true;
	}

	LinkedBlockingQueue<Table> tables = new LinkedBlockingQueue<>(100);

	private <T> T table(String name, Function<Table, T> using) {
		Table t = tables.poll();
		if (null == t) try {
			t = hconn.getTable(TableName.valueOf(name), Hbases.ex);
		} catch (IOException e1) {
			return null;
		}
		try {
			return using.apply(t);
		} finally {
			if (!tables.offer(t)) try {
				t.close();
			} catch (IOException e) {
				logger().error("Hbase close fail", e);
			}
		}
	}
}
