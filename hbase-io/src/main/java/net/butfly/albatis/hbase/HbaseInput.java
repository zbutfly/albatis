package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import net.butfly.albacore.io.InputQueueImpl;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;

public class HbaseInput<T> extends InputQueueImpl<T, HbaseResult> {
	private static final long serialVersionUID = 8805176327882596072L;
	protected final Connection connect;
	protected final String tableName;
	protected final Table table;
	protected final ResultScanner scaner;
	private final Converter<HbaseResult, T> conv;

	public HbaseInput(final String table, final Converter<HbaseResult, T> conv, final Filter... filter) throws IOException {
		super("hbase-input-queue");
		this.tableName = table;
		this.conv = conv;
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
	protected T dequeueRaw() {
		try {
			return conv.apply(new HbaseResult(tableName, scaner.next()));
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public List<T> dequeue(long batchSize) {
		List<T> l = new ArrayList<>();
		Result[] results;
		try {
			results = scaner.next((int) batchSize);
		} catch (IOException e) {
			return new ArrayList<>();
		}
		for (int i = 0; i < results.length; i++) {
			Result r;
			try {
				r = scaner.next();
			} catch (IOException e) {
				logger.error("HBase read failure", e);
				continue;
			}
			T rr = conv.apply(new HbaseResult(tableName, r));
			if (null != rr) l.add(rr);
		}
		return l;
	}

	public List<T> get(List<Get> gets) throws IOException {
		return Collections.transform(r -> conv.apply(new HbaseResult(tableName, r)), table.get(gets));
	}
}
