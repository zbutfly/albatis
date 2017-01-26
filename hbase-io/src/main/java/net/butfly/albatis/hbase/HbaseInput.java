package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerTimeoutException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import net.butfly.albacore.io.InputImpl;
import net.butfly.albacore.lambda.Supplier;
import net.butfly.albacore.utils.Collections;

public final class HbaseInput extends InputImpl<HbaseResult> {
	protected final Connection connect;
	protected final String tableName;
	protected final Table table;
	protected final ResultScanner scaner;
	private final ReentrantReadWriteLock scanerLock;
	private final AtomicBoolean ended;
	private final Supplier<ResultScanner> scaning;

	public HbaseInput(String name, final String table, final Filter... filter) throws IOException {
		super(name);
		this.tableName = table;
		Properties p = new Properties();
		p.setProperty(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, Integer.toString(Integer.MAX_VALUE));
		this.connect = Hbases.connect(p);
		this.table = connect.getTable(TableName.valueOf(table));
		if (null != filter && filter.length > 0) {
			if (filter.length == 1) scaning = () -> {
				try {
					return this.table.getScanner(new Scan().setFilter(filter[0]));
				} catch (Exception e) {
					logger().error("ResultScaner create failure", e);
					return null;
				}
			};
			else {
				FilterList all = new FilterList();
				for (Filter f : filter)
					all.addFilter(f);
				scaning = () -> {
					try {
						return this.table.getScanner(new Scan().setFilter(all));
					} catch (Exception e) {
						logger().error("ResultScaner create failure", e);
						return null;
					}
				};
			}
		} else scaning = () -> {
			try {
				return this.table.getScanner(new Scan());
			} catch (Exception e) {
				logger().error("ResultScaner create failure", e);
				return null;
			}
		};
		scaner = scaning.get();
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
			scaner.close();
			table.close();
			connect.close();
		} catch (IOException e) {
			logger().error("Close failure", e);
		}
	}

	@Override
	public HbaseResult dequeue(boolean block) {
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
	public List<HbaseResult> dequeue(long batchSize) {
		if (ended.get()) return new ArrayList<>();
		if (scanerLock.writeLock().tryLock()) {
			try {
				List<HbaseResult> l = Collections.map(Arrays.asList(scaner.next((int) batchSize)), r -> new HbaseResult(tableName, r));
				ended.set(l.isEmpty());
				return l;
			} catch (ScannerTimeoutException ex) {
				return new ArrayList<>();
			} catch (IOException e) {
				return new ArrayList<>();
			} finally {
				scanerLock.writeLock().unlock();
			}
		} else return new ArrayList<>();
	}

	public List<HbaseResult> get(List<Get> gets) throws IOException {
		return Collections.map(Arrays.asList(table.get(gets)), r -> new HbaseResult(tableName, r));
	}
}
