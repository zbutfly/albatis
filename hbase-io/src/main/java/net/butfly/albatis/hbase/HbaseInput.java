package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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

import net.butfly.albacore.io.InputImpl;
import net.butfly.albacore.utils.collection.Maps;

public final class HbaseInput extends InputImpl<HbaseResult> {
	protected final Connection hconn;
	protected final String tname;
	protected final Table htable;

	protected ResultScanner scaner = null;
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

	public List<HbaseResult> get(List<Get> gets) throws IOException {
		try (Table t = hconn.getTable(TableName.valueOf(tname));) {
			for (int i = 1;; i++)
				try {
					return io.list(Stream.of(t.get(gets)).parallel().map(r -> new HbaseResult(tname, r)));
				} catch (Exception ex) {
					logger().warn("Hbase get failure and retry [" + i + "] times", ex);
				}
		}
	}

	public HbaseResult get(Get get) throws IOException {
		try (Table t = hconn.getTable(TableName.valueOf(tname));) {
			for (int i = 1;; i++)
				try {
					return new HbaseResult(tname, t.get(get));
				} catch (Exception ex) {
					logger().warn("Hbase get failure and retry [" + i + "] times", ex);
				}
		}
	}
}
