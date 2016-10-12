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

public class HbaseOutput extends OutputQueueImpl<HbaseMessage, Put> implements OutputQueue<HbaseMessage> {
	private static final long serialVersionUID = 2141020043117686747L;
	private final Connection connect;
	private final Table table;

	public HbaseOutput(final String table) throws IOException {
		super("hbase-output-queue");
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
	protected boolean enqueueRaw(HbaseMessage r) {
		try {
			table.put(r.put());
			return true;
		} catch (IOException ex) {
			logger.error("Hbase output failure", ex);
			return false;
		}
	}

	@Override
	public long enqueue(Iterator<HbaseMessage> iter) {
		List<Put> puts = new ArrayList<>();
		while (iter.hasNext()) {
			HbaseMessage m = iter.next();
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

	@Override
	public long enqueue(HbaseMessage... r) {
		if (null == r || r.length == 0) return 0;
		List<Put> puts = new ArrayList<>();
		for (int i = 0; i < r.length; i++) {
			HbaseMessage m = r[i];
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
