package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Streams;
import net.butfly.albatis.io.KeyOutput;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Message.Op;

public final class HbaseOutput extends Namedly implements KeyOutput<String, Message> {
	public static final int SUGGEST_BATCH_SIZE = 200;
	private final HbaseConnection hconn;

	public HbaseOutput(String name, HbaseConnection hconn) throws IOException {
		super(name);
		this.hconn = hconn;
		open();
	}

	@Override
	public void enqueue(String table, Stream<Message> msgs) {
		Map<String, Message> map = new ConcurrentHashMap<>();
		List<Pair<Message, ? extends Row>> l = incs(table, msgs).stream().filter(Streams.NOT_NULL).map(v -> new Pair<>(v, Hbases.Results
				.put(v))).filter(p -> null != p && null != p.v2()).peek(p -> map.put(Bytes.toString(p.v2().getRow()), p.v1())).collect(
						Collectors.toList());
		if (l.isEmpty()) return;
		List<Message> vs = l.stream().map(v -> v.v1()).collect(Collectors.toList());
		List<? extends Row> puts = l.stream().map(v -> v.v2()).collect(Collectors.toList());
		Object[] results = new Object[l.size()];
		try {
			hconn.table(table).batchCallback(puts, results, (region, row, result) -> {
				if (result instanceof Result) succeeded(1);
				else {
					Message m = map.get(Bytes.toString(row));
					logger().debug(() -> "Hbase failed on: " + m.toString(), result instanceof Throwable ? (Throwable) result
							: new RuntimeException("Unknown hbase return [" + result.getClass() + "]: " + result.toString()));
					failed(Streams.of(new Message[] { m }));
				}
			});
		} catch (Exception ex) {
			logger().warn(name() + " write failed [" + Exceptions.unwrap(ex).getMessage() + "], [" + l.size() + "] into fails.");
			List<Message> fails = new CopyOnWriteArrayList<>();
			for (int i = 0; i < results.length; i++)
				if (results[i] instanceof Result) succeeded(1);
				else fails.add(vs.get(i));
			failed(fails.parallelStream());
		} finally {}
	}

	private List<Message> incs(String table, Stream<Message> values) {
		Map<Boolean, List<Message>> ms = values.parallel().filter(Streams.NOT_NULL).collect(Collectors.partitioningBy(m -> m
				.op() == Op.INCREASE));
		List<Message> alls = ms.remove(Boolean.FALSE);
		Map<String, List<Message>> incByKeys = ms.get(Boolean.TRUE).parallelStream().collect(Collectors.groupingBy(m -> m.key()));
		for (Map.Entry<String, List<Message>> e : incByKeys.entrySet()) {
			Message merge = new Message(table, e.getKey());
			for (Message m : e.getValue())
				for (Map.Entry<String, Object> f : m.entrySet())
					merge.compute(f.getKey(), (fn, v) -> lvalue(v) + lvalue(e.getValue()));
			alls.add(merge);
		}
		return alls;
	}

	private long lvalue(Object o) {
		return null != o && Number.class.isAssignableFrom(o.getClass()) ? ((Number) o).longValue() : 0;
	}

	@Override
	public String partition(Message v) {
		return v.table();
	}
}
