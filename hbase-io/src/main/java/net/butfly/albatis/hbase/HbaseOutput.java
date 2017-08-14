package net.butfly.albatis.hbase;

import static com.hzcominfo.albatis.nosql.Connection.PARAM_KEY_BATCH;
import static net.butfly.albacore.io.utils.Streams.list;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.util.Bytes;

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

	private final Callback<Result> callback = (region, row, result) -> //
	logger().trace(() -> "HBase put [" + Bytes.toString(row) + "] callbacked.");

	@Override
	protected long write(String table, Stream<HbaseResult> values, Consumer<Collection<HbaseResult>> failing, Consumer<Long> committing,
			int retry) {
		List<HbaseResult> vs = list(values);
		List<Put> puts = new ArrayList<>();
		for (HbaseResult r : vs)
			if (null != r) puts.add(r.forWrite());
		if (puts.isEmpty()) return 0;
		Object[] results = new Object[puts.size()];
		long succ = 0;
		try {
			if (logger().isTraceEnabled()) table(table).batchCallback(puts, results, callback);
			else table(table).batch(puts, results);
		} catch (Exception ex) {
			logger().warn(name() + " write failed [" + Exceptions.unwrap(ex).getMessage() + "], [" + puts.size() + "] into failover.");
			failing.accept(list(puts, p -> new HbaseResult(table, p)));
		} finally {
			List<HbaseResult> fails = new ArrayList<>();
			for (int i = 0; i < results.length; i++) {
				if (results[i] instanceof Result) succ++;
				else fails.add(vs.get(i));
			}
			committing.accept((long) succ);
			failing.accept(fails);
		}
		return results.length;
	}
}
