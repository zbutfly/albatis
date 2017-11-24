package net.butfly.albatis.hbase;

import static net.butfly.albacore.paral.split.SplitEx.list;
import static net.butfly.albacore.utils.collection.Streams.map;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.steam.Steam;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.KeyOutput;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Message.Op;

/**
 * Subject (embedded document serialized as BSON with prefixed column name) writer to hbase.
 */
public final class HbaseOutput extends OutputBase<Message> {
	public static final @HbaseProps String MAX_CONCURRENT_OP_PROP_NAME = HbaseProps.OUTPUT_CONCURRENT_OPS;
	public static final int MAX_CONCURRENT_OP_DEFAULT = Integer.MAX_VALUE;
	public static final int SUGGEST_BATCH_SIZE = 200;
	private final HbaseConnection hconn;

	public HbaseOutput(String name, HbaseConnection hconn, Function<Map<String, Object>, byte[]> ser) throws IOException {
		super(name);
		this.hconn = hconn;
	}

	@Override
	public void enqueue(String table, Steam<Message> msgs) {
		Map<String, Message> map = Maps.of();
		List<Pair<Message, ? extends Row>> l = map(incs(table, msgs), m -> new Pair<>(m, Hbases.Results.put(m)), p -> {
			boolean b = null != p && null != p.v2();
			if (b) map.put(Bytes.toString(p.v2().getRow()), p.v1());
			return b;
		}, Collectors.toList());
		if (l.isEmpty()) return;
		List<Message> vs = map(l, v -> v.v1(), Collectors.toList());
		List<? extends Row> puts = map(l, v -> v.v2(), Collectors.toList());
		Object[] results = new Object[l.size()];
		try {
			hconn.table(table).batchCallback(puts, results, (region, row, result) -> {
				if (result instanceof Result) succeeded(1);
				else {
					Message m = map.get(Bytes.toString(row));
					logger().debug(() -> "Hbase failed on: " + m.toString(), result instanceof Throwable ? (Throwable) result
							: new RuntimeException("Unknown hbase return [" + result.getClass() + "]: " + result.toString()));
					failed(Steam.of(m));
				}
			});
		} catch (Exception ex) {
			logger().warn(name() + " write failed [" + Exceptions.unwrap(ex).getMessage() + "], [" + l.size() + "] into fails.");
			List<Message> fails = new CopyOnWriteArrayList<>();
			for (int i = 0; i < results.length; i++)
				if (results[i] instanceof Result) succeeded(1);
				else fails.add(vs.get(i));
			failed(Steam.of(fails));
		} finally {}
	}

	private List<Message> incs(String table, Steam<Message> values) {
		List<Message> upds = list();
		Map<String, List<Message>> incByKeys = Maps.of();
		for (Message m : values.list())
			if (m.op() == Op.INCREASE) {
				incByKeys.compute(m.key(), (k, l) -> {
					if (null == l) l = list();
					l.add(m);
					return l;
				});
			} else upds.add(m);
		for (Map.Entry<String, List<Message>> e : incByKeys.entrySet()) {
			Message merge = new Message(table, e.getKey()).op(Op.INCREASE);
			for (Message m : e.getValue())
				for (Map.Entry<String, Object> f : m.entrySet())
					merge.compute(f.getKey(), (fn, v) -> lvalue(v) + lvalue(f.getValue()));
			for (String k : merge.keySet())
				if (((Long) merge.get(k)).longValue() <= 0) merge.remove(k);
			if (!merge.isEmpty()) upds.add(merge);
		}
		return upds;
	}

	private long lvalue(Object o) {
		return null != o && Number.class.isAssignableFrom(o.getClass()) ? ((Number) o).longValue() : 0;
	}

	@Override
	public String partition(Message v) {
		return v.table();
	}
}
