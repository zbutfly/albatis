package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
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

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.Streams;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Maps;

public final class HbaseInput extends Namedly implements Input<HbaseResult> {
	private static final int MAX_RETRIES = Integer.MAX_VALUE;
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
	public void dequeue(Consumer<Stream<HbaseResult>> using, long batchSize) {
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
				if (rs.length > 0) using.accept(Stream.of(rs).map(r -> new HbaseResult(tname, r)));
			}
		}
	}

	public List<HbaseResult> get(List<Get> gets) {
		if (gets == null || !gets.iterator().hasNext()) return new ArrayList<>();
		AtomicReference<List<Result>> ref = new AtomicReference<>();
		long now = System.currentTimeMillis();
		table(tname, t -> {
			try {
				ref.set(Arrays.asList(t.get(gets)));
			} catch (Exception ex) {
				long now1 = System.currentTimeMillis();
				List<Result> l = null;
				AtomicInteger retries = new AtomicInteger();
				try {
					l = IO.list(Streams.of(gets, true).map(g -> get(g, retries)));
				} finally {
					logger().debug("Hbase batch not ready, fetch [" + (null == l ? "UNKNOWN" : l.size()) + "] items each by each in ["
							+ (System.currentTimeMillis() - now1) + "] ms in [" + retries.get() + "] retries");
				}
				ref.set(l);
			}
		});
		List<HbaseResult> rs = IO.list(ref.get(), r -> new HbaseResult(tname, r));
		logger().trace(() -> "Hbase batch get: " + (System.currentTimeMillis() - now) + " ms, total [" + gets.size() + " gets]/[" + Texts
				.formatKilo(HbaseResult.sizes(rs), " bytes") + "]/[" + HbaseResult.cells(rs) + " cells].");
		return rs;
	}

	private Result getByScan(Get get, AtomicInteger retries) {
		AtomicReference<Result> ref = new AtomicReference<>();
		long now = System.currentTimeMillis();
		table(tname, t -> {
			Result r;
			boolean doNotRetry;
			int retry = 0;
			do {
				doNotRetry = true;
				Scan s = new Scan().setRowPrefixFilter(get.getRow());
				if (get.hasFamilies()) for (byte[] cf : get.familySet())
					s.addFamily(cf);
				Filter f = get.getFilter();
				if (null != f) s.setFilter(f);
				ResultScanner sc;
				try {
					sc = t.getScanner(s);
				} catch (IOException ex) {
					logger().error("Hbase get failed on retry #" + retry + ": [" + get.toString() + "], spent [" + (System
							.currentTimeMillis() - now) + " ms]==>\n\t" + ex.getMessage());
					return;
				}
				try {
					r = sc.next();
				} catch (IOException ex) {
					r = null;
					doNotRetry = isDoNotRetry(ex);
					if (doNotRetry) {
						logger().error("Hbase get failed on retry #" + retry + ": [" + get.toString() + "], spent [" + (System
								.currentTimeMillis() - now) + " ms]==>\n\t" + ex.getMessage());
						return;
					}
				} finally {
					sc.close();
				}
			} while (null == r && !doNotRetry && retry++ < MAX_RETRIES && opened());
			if (null != retries) retries.addAndGet(retry);
			ref.set(r);
		});
		Result r = ref.get();
		return null == r ? null : r;
	}

	public Result get(Get get, AtomicInteger retries) {
		AtomicReference<Result> ref = new AtomicReference<>();
		table(tname, t -> {
			Result r;
			try {
				r = t.get(get);
			} catch (Exception ex) {
				r = getByScan(get, retries);
			}
			ref.set(r);
		});
		return ref.get();
	}

	private boolean isDoNotRetry(Throwable th) {
		while (!(th instanceof RemoteWithExtrasException) && th.getCause() != null && th.getCause() != th)
			th = th.getCause();
		if (th instanceof RemoteWithExtrasException) return ((RemoteWithExtrasException) th).isDoNotRetry();
		else return true;
	}

	LinkedBlockingQueue<Table> tables = new LinkedBlockingQueue<>(100);

	private void table(String name, Consumer<Table> using) {
		Table t = tables.poll();
		if (null == t) try {
			t = hconn.getTable(TableName.valueOf(name), Hbases.ex);
		} catch (IOException e1) {
			return;
		}
		try {
			using.accept(t);
		} finally {
			if (!tables.offer(t)) try {
				t.close();
			} catch (IOException e) {
				logger().error("Hbase close fail", e);
			}
		}
	}
}
