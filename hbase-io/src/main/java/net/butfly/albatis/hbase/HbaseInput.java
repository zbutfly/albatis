package net.butfly.albatis.hbase;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.utils.Collections;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HbaseInput extends Input<HbaseResult> {
	private static final long serialVersionUID = 8805176327882596072L;
	protected final Connection connect;
	protected final String tableName;
	protected final Table table;
	protected final ResultScanner scaner;
	private final ReentrantReadWriteLock scanerLock;
	private final AtomicBoolean ended;

	public HbaseInput(String name, final String table, final Filter... filter) throws IOException {
		super(name);
		this.tableName = table;
		this.connect = Hbases.connect();
		this.table = connect.getTable(TableName.valueOf(table));
		if (null != filter && filter.length > 0) {
			if (filter.length == 1) this.scaner = this.table.getScanner(new Scan().setFilter(filter[0]));
			else {
				FilterList all = new FilterList();
				for (Filter f : filter)
					all.addFilter(f);
				this.scaner = this.table.getScanner(new Scan().setFilter(all));
			}
		} else this.scaner = this.table.getScanner(new Scan());
		scanerLock = new ReentrantReadWriteLock();
		ended = new AtomicBoolean(false);
	}

	@Override
	public void close() {
		super.close();
		try {
			scaner.close();
			table.close();
			connect.close();
		} catch (IOException e) {
			throw new RuntimeException("HBase close failure", e);
		}
	}

	@Override
	public HbaseResult dequeue0() {
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
		List<HbaseResult> l;
		if (ended.get() || scanerLock.writeLock().tryLock()) {
			try {
				l = Collections.transform(Arrays.asList(scaner.next((int) batchSize)), r -> new HbaseResult(tableName, r));
				ended.set(l.isEmpty());
			} catch (IOException e) {
				l = new ArrayList<>();
			} finally {
				scanerLock.writeLock().unlock();
			}
		} else l = new ArrayList<>();
		return l;
	}

	public List<HbaseResult> get(List<Get> gets) throws IOException {
		return Collections.transform(r -> new HbaseResult(tableName, r), table.get(gets));
	}
}
