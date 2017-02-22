package net.butfly.albatis.hbase;

import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import net.butfly.albacore.io.Streams;
import net.butfly.albacore.io.faliover.Failover.FailoverException;
import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.utils.Configs;

public final class HbaseOutput extends FailoverOutput<String, HbaseResult> {
	private final static int BATCH_SIZE = Integer.parseInt(Configs.MAIN_CONF.getOrDefault("albatis.io.hbase.batch.size", "500"));
	private final Connection connect;
	private final Map<String, Table> tables;

	public HbaseOutput(String name, String failoverPath) throws IOException {
		super(name, b -> new HbaseResult(b), failoverPath, BATCH_SIZE);
		connect = Hbases.connect();
		tables = new ConcurrentHashMap<>();
		open();
	}

	private Table table(String table) {
		return tables.computeIfAbsent(table, t -> {
			try {
				return connect.getTable(TableName.valueOf(t));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	protected void closeInternal() {
		for (String k : tables.keySet())
			try {
				Table t = tables.remove(k);
				if (null != t) t.close();
			} catch (IOException e) {
				logger().error("Hbase table [" + k + "] close failure", e);
			}
		try {
			synchronized (connect) {
				if (!connect.isClosed()) connect.close();
			}
		} catch (IOException e) {
			logger().error("Hbase close failure", e);
		}
	}

	@Override
	protected int write(String table, Collection<HbaseResult> values) throws FailoverException {
		List<Put> puts = io.list(values, HbaseResult::forWrite);
		try {
			table(table).put(puts);
		} catch (Exception e) {
			throw new FailoverException(io.collect(Streams.of(values), Collectors.toConcurrentMap(r -> r, r -> {
				String m = unwrap(e).getMessage();
				if (null == m) m = "Unknown message";
				return m;
			})));
		}
		return puts.size();
	}
}
