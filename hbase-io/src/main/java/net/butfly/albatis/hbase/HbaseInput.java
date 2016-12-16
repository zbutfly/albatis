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

import net.butfly.albacore.io.Input;
import net.butfly.albacore.utils.Collections;

public class HbaseInput extends Input<HbaseResult> {
	private static final long serialVersionUID = 8805176327882596072L;
	protected final Connection connect;
	protected final String tableName;
	protected final Table table;
	protected final ResultScanner scaner;

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
	protected void closing() {
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
	public List<HbaseResult> dequeue(long batchSize) {
		List<HbaseResult> l = new ArrayList<>();
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
			l.add(new HbaseResult(tableName, r));
		}
		return l;
	}

	public List<HbaseResult> get(List<Get> gets) throws IOException {
		return Collections.transform(r -> new HbaseResult(tableName, r), table.get(gets));
	}
}
