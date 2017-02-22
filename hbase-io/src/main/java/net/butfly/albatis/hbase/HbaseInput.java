package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
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
		htable = hconn.getTable(TableName.valueOf(table));

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
		htable = hconn.getTable(TableName.valueOf(table));

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
			closeScaning();
		} catch (IOException e) {
			logger().error("Close failure", e);
		}
		try {
			hconn.close();
		} catch (IOException e) {
			logger().error("Close failure", e);
		}
	}

	@Override
	public boolean empty() {
		return ended == null || ended.get();
	}

	@Override
	public HbaseResult dequeue() {
		try {
			return new HbaseResult(tname, scaner.next());
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public Stream<HbaseResult> dequeue(long batchSize) {
		if (!ended.get() && scanerLock.writeLock().tryLock()) {
			try {
				Result[] rs = scaner.next((int) batchSize);
				ended.set(rs == null || rs.length == 0);
				return Stream.of(rs).map(r -> new HbaseResult(tname, r));
			} catch (Exception ex) {
				logger().warn("Hbase failure", ex);
			} finally {
				scanerLock.writeLock().unlock();
			}
		}
		return Stream.empty();
	}

	public void closeScaning() throws IOException {
		if (null != scaner) scaner.close();
		htable.close();
	}

	private static Random random = new Random();

	public List<HbaseResult> get(List<Get> gets, long batchSize) {
		int batchs = (int) (gets.size() / batchSize) + 1;
		return batchs > 1 ? IO.list(Streams.of(IO.collect(gets, Collectors.groupingBy(g -> random.nextInt(batchs))).values()).map(this::get)
				.flatMap(Streams::of)) : get(gets);
	}

	public List<HbaseResult> get(List<Get> gets) {
		long now = System.currentTimeMillis();
		List<HbaseResult> results;
		try (Table t = hconn.getTable(TableName.valueOf(tname));) {
			try {
				results = io.list(Stream.of(t.get(gets)).parallel().map(r -> new HbaseResult(tname, r)));
			} catch (Exception ex) {
				results = io.list(gets, this::get);
			}
		} catch (IOException e) {
			results = new ArrayList<>();
		}
		logger().trace("Hbase batch get: " + (System.currentTimeMillis() - now) + " ms, total [" + gets.size() + " gets]/[" + Texts
				.formatKilo(HbaseResult.sizes(results), " bytes") + "]/[" + HbaseResult.cells(results) + " cells], ");
		return results;
	}

	public HbaseResult get(Get get) {
		try (Table t = hconn.getTable(TableName.valueOf(tname));) {
			for (int i = 0; i < MAX_RETRIES; i++)
				try {
					return new HbaseResult(tname, t.get(get));
				} catch (Exception ex) {
					logger().warn("Hbase get failure to retry#" + (i + 1), ex);
				}
		} catch (IOException e) {
			logger().warn("Hbase get failure and ignore", e);
		}
		return null;
	}
}
