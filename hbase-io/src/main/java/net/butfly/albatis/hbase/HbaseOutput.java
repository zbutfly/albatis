package net.butfly.albatis.hbase;

import static com.hzcominfo.albatis.nosql.Connection.PARAM_KEY_BATCH;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.utils.Exceptions;

public final class HbaseOutput extends FailoverOutput<String, HbaseResult> {
	private final Connection connect;
	private final Map<String, Table> tables;

	public HbaseOutput(String name, URISpec uri, String failoverPath) throws IOException {
		super(name, HbaseResult::new, failoverPath, null == uri ? 200 : Integer.parseInt(uri.getParameter(PARAM_KEY_BATCH, "200")));
		connect = Hbases.connect(null == uri ? null : uri.getParameters());
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
	protected long write(String table, Stream<HbaseResult> values, Consumer<Collection<HbaseResult>> failing, Consumer<Long> committing,
			int retry) {
		List<Put> puts = IO.list(values, HbaseResult::forWrite);
		if (puts.isEmpty()) return 0;
		long rr = 0;
		try {
			table(table).put(puts);
			rr = puts.size();
		} catch (Exception ex) {
			logger().warn(name() + " write failed [" + Exceptions.unwrap(ex).getMessage() + "], [" + puts.size() + "] into failover.");
			failing.accept(IO.list(puts, p -> new HbaseResult(table, p)));
		} finally {
			committing.accept(rr);
		}
		return rr;
	}
}
