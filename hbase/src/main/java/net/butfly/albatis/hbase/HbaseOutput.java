package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import net.butfly.albacore.io.OutputQueue;
import net.butfly.albacore.io.OutputQueueImpl;
import net.butfly.albacore.lambda.Converter;

public final class HbaseOutput<T> extends OutputQueueImpl<T, HbaseResult> implements OutputQueue<T> {
	private static final long serialVersionUID = 2141020043117686747L;
	private final Connection connect;
	private final Table table;

	public HbaseOutput(final String table, Converter<T, HbaseResult> conv) throws IOException {
		super("hbase-output-queue", conv);
		this.connect = Hbases.connect();
		this.table = connect.getTable(TableName.valueOf(table));
	}

	@Override
	public void close() {
		try {
			connect.close();
		} catch (IOException e) {
			throw new RuntimeException("HBase close failure", e);
		}
	}

	@Override
	protected boolean enqueueRaw(T r) {
		try {
			table.put(conv.apply(r).put());
			return true;
		} catch (IOException ex) {
			logger.error("Hbase output failure", ex);
			return false;
		}
	}

	@Override
	public long enqueue(Iterator<T> iter) {
		List<Put> puts = new ArrayList<>();
		while (iter.hasNext()) {
			HbaseResult m = conv.apply(iter.next());
			if (null != m) try {
				puts.add(m.put());
			} catch (IOException e) {
				logger.error("Hbase message to put failure, message ignored", e);
			}
		}
		try {
			table.put(puts);
			return puts.size();
		} catch (IOException e) {
			logger.error("Hbase output failure", e);
			return 0;
		}
	}
}
