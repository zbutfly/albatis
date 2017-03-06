package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
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

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.Streams;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

public final class HbaseInput extends Namedly implements Input<HbaseResult> {
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

		open();
	}

	@Override
	public void close() {
		Input.super.close(this::closeHbase);
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

//	AtomicInteger parallelism = new AtomicInteger();

	public List<HbaseResult> get(List<Get> gets) {
//		logger().debug("[Hbase get parallelism: " + parallelism.incrementAndGet() + "]");
//		try {
			AtomicReference<List<HbaseResult>> ref = new AtomicReference<>();
			long now = System.currentTimeMillis();
			table(tname, t -> {
				try {
					ref.set(IO.list(Streams.of(t.get(gets)).map(r -> new HbaseResult(tname, r))));
				} catch (Exception ex) {
					logger().warn("Hbase batch get fail, try slow single fetching.");
					ref.set(IO.list(Streams.of(gets, true).map(this::get)));
				}
				logger().trace("Hbase batch get: " + (System.currentTimeMillis() - now) + " ms, total [" + gets.size() + " gets]/[" + Texts
						.formatKilo(HbaseResult.sizes(ref.get()), " bytes") + "]/[" + HbaseResult.cells(ref.get()) + " cells].");
			});
			return ref.get();
//		} finally {
//			parallelism.getAndDecrement();
//		}
	}

	public HbaseResult get(Get get) {
//		logger().debug("[Hbase get parallelism: " + parallelism.incrementAndGet() + "]");
//		try {
			AtomicReference<HbaseResult> ref = new AtomicReference<>();
			table(tname, t -> {
				try {
					ref.set(new HbaseResult(tname, t.get(get)));
				} catch (Exception ex) {
					Logger l = logger();
					if (l.isTraceEnabled()) l.error("Hbase get failed on: " + get.toString(), ex);
					else l.error("Hbase get failed on: " + get.toString() + "\n\t[" + ex.getMessage() + "]");
				}
			});
			return ref.get();
//		} finally {
//			parallelism.getAndDecrement();
//		}
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
