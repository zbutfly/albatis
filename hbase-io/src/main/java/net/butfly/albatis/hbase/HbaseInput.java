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

import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.InputImpl;
import net.butfly.albacore.io.Streams;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Maps;

public final class HbaseInput extends InputImpl<HbaseResult> {
	private final Connection hconn;
	private final String tname;
	private final Table htable;
	private final static int MAX_RETRIES = Integer.parseInt(Configs.MAIN_CONF.getOrDefault("albatis.io.hbase.retry", "5"));
	protected final static int BATCH_SIZE = Integer.parseInt(Configs.MAIN_CONF.getOrDefault("albatis.io.hbase.batch.size", "500"));

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
		super.close(this::closeHbase);
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
	protected HbaseResult dequeue() {
		try {
			return new HbaseResult(tname, scaner.next());
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public void dequeue(Consumer<Stream<HbaseResult>> using, long batchSize) {
		if (!ended.get() && scanerLock.writeLock().tryLock()) {
			try {
				Result[] rs = scaner.next((int) batchSize);
				ended.set(rs == null || rs.length == 0);
				using.accept(Stream.of(rs).map(r -> new HbaseResult(tname, r)));
			} catch (Exception ex) {
				logger().warn("Hbase failure", ex);
			} finally {
				scanerLock.writeLock().unlock();
			}
		}
	}

	public List<HbaseResult> get(List<Get> gets) {
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
	}

	public HbaseResult get(Get get) {
		AtomicReference<HbaseResult> ref = new AtomicReference<>();
		for (int i = 0; i < MAX_RETRIES; i++) {
			int ii = i;
			table(tname, t -> {
				try {
					ref.set(new HbaseResult(tname, t.get(get)));
				} catch (Exception ex) {
					logger().warn("Hbase get failure to retry#" + (ii + 1), ex);
				}
			});
		}
		return ref.get();
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
