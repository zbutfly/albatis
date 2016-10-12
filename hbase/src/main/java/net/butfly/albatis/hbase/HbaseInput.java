package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import net.butfly.albacore.io.InputQueueImpl;

public class HbaseInput extends InputQueueImpl<HbaseMessage, Result> {
	private static final long serialVersionUID = 8805176327882596072L;
	private final Connection connect;
	private final String tableName;
	private final Table table;
	private final ResultScanner scaner;

	public HbaseInput(final String table, final Filter... filter) throws IOException {
		super("hbase-input-queue");
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
	}

	@Override
	public void close() {
		try {
			scaner.close();
			table.close();
			connect.close();
		} catch (IOException e) {
			throw new RuntimeException("HBase close failure", e);
		}
	}

	@Override
	protected HbaseMessage dequeueRaw() {
		try {
			return new HbaseMessage(tableName, scaner.next());
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public List<HbaseMessage> dequeue(long batchSize) {
		List<HbaseMessage> l = new ArrayList<>();
		Result[] results;
		try {
			results = scaner.next((int) batchSize);
		} catch (IOException e) {
			return new ArrayList<>();
		}
		for (int i = 0; i < results.length; i++)
			l.add(new HbaseMessage(tableName, results[i]));
		return l;
	}
}
