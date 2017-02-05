package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.Arrays;
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
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.collection.Maps;

public final class HbaseInput extends InputImpl<HbaseResult> {
	protected final Connection connect;
	protected final String tableName;
	protected final Table table;
	protected final ResultScanner scaner;
	private final ReentrantReadWriteLock scanerLock;
	private final AtomicBoolean ended;

	public HbaseInput(String name, final String table, final Filter... filter) throws IOException {
		super(name);
		this.tableName = table;
		connect = Hbases.connect(Maps.of(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, Integer.toString(Integer.MAX_VALUE)));
		this.table = connect.getTable(TableName.valueOf(table));
		if (null != filter && filter.length > 0) {
			if (filter.length == 1) scaner = this.table.getScanner(new Scan().setFilter(filter[0]));
			else {
				FilterList all = new FilterList();
				for (Filter f : filter)
					all.addFilter(f);
				scaner = this.table.getScanner(new Scan().setFilter(all));
			}
		} else scaner = this.table.getScanner(new Scan());;
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
			connect.close();
		} catch (IOException e) {
			logger().error("Close failure", e);
		}
	}

	@Override
	public HbaseResult dequeue() {
		try {
			return new HbaseResult(tableName, scaner.next());
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public boolean empty() {
		return ended.get();
	}

	@Override
	public Stream<HbaseResult> dequeue(long batchSize) {
		if (!ended.get() && scanerLock.writeLock().tryLock()) {
			try {
				List<Result> l = Arrays.asList(scaner.next((int) batchSize));
				ended.set(l.isEmpty());
				return l.parallelStream().filter(t -> t != null).map(r -> new HbaseResult(tableName, r));
			} catch (Exception ex) {
				logger().warn("Hbase failure", ex);
			} finally {
				scanerLock.writeLock().unlock();
			}
		}
		return Stream.empty();
	}

	public void closeScaning() throws IOException {
		scaner.close();
		table.close();
	}

	public List<HbaseResult> get(List<Get> gets) throws IOException {
		try (Table t = connect.getTable(TableName.valueOf(tableName));) {
			return Collections.map(Arrays.asList(t.get(gets)), r -> new HbaseResult(tableName, r));
		}
	}

	public List<HbaseResult> get(String table, List<Get> gets) throws IOException {
		try (Table t = connect.getTable(TableName.valueOf(table));) {
			return Collections.map(Arrays.asList(t.get(gets)), r -> new HbaseResult(tableName, r));
		}
	}
}
