package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import net.butfly.albacore.io.EnqueueException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Streams;
import net.butfly.albatis.io.KeyOutput;
import net.butfly.albatis.io.Message;

public final class HbaseOutput extends KeyOutput<String, Message> {
	public static final int SUGGEST_BATCH_SIZE = 200;
	private final Connection connect;
	private final Map<String, Table> tables;

	public HbaseOutput(String name, URISpec uri) throws IOException {
		super(name);
		// batchSize = null == uri ? SUGGEST_BATCH_SIZE
		// : Integer.parseInt(uri.getParameter(PARAM_KEY_BATCH, Integer.toString(SUGGEST_BATCH_SIZE)));
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
	public void close() {
		super.close();
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
	public long enqueue(String table, Stream<Message> values) throws EnqueueException {
		List<Pair<Message, Put>> l = values.parallel().filter(Streams.NOT_NULL).map(v -> new Pair<>(v, Hbases.Results.put(v))).filter(
				p -> null != p.v2()).collect(Collectors.toList());
		if (l.isEmpty()) return 0;
		List<Message> vs = l.stream().map(v -> v.v1()).collect(Collectors.toList());
		List<Put> puts = l.stream().map(v -> v.v2()).collect(Collectors.toList());
		Object[] results = new Object[puts.size()];
		EnqueueException eex = new EnqueueException();
		try {
			table(table).batch(puts, results);
		} catch (Exception ex) {
			logger().warn(name() + " write failed [" + Exceptions.unwrap(ex).getMessage() + "], [" + puts.size() + "] into failover.");
			eex.fails(vs);
		} finally {
			for (int i = 0; i < results.length; i++) {
				if (results[i] instanceof Result) eex.success(1);
				else eex.fail(vs.get(i), results[i] instanceof Throwable ? (Throwable) results[i]
						: new RuntimeException("Unknown hbase return [" + results[i].getClass() + "]: " + results[i].toString()));
			}
		}
		if (eex.empty()) return eex.success();
		else throw eex;
	}

	@Override
	protected String partitionize(Message v) {
		return v.key();
	}
}
