package net.butfly.albatis.hbase;

import static net.butfly.albacore.utils.collection.Colls.list;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Message.Op;
import net.butfly.albatis.io.SafeKeyOutput;

public final class HbaseOutput extends SafeKeyOutput<String, Message> {
	public static final @HbaseProps String MAX_CONCURRENT_OP_PROP_NAME = HbaseProps.OUTPUT_CONCURRENT_OPS;
	public static final int MAX_CONCURRENT_OP_DEFAULT = Integer.MAX_VALUE;
	public static final int SUGGEST_BATCH_SIZE = 200;
	private final HbaseConnection hconn;

	public HbaseOutput(String name, HbaseConnection hconn, Function<Map<String, Object>, byte[]> ser) throws IOException {
		super(name);
		this.hconn = hconn;
		open();
	}

	@Override
	public void enqueue(String table, Sdream<Message> msgs, AtomicInteger ops) {
		ops.incrementAndGet();
		Map<String, Message> map = Maps.of();
		List<Pair<Message, Row>> l = Sdream.of(incs(table, msgs)).map(m -> new Pair<>(m, Hbases.Results.put(m))).filter(p -> {
			boolean b = null != p && null != p.v2();
			if (b) map.put(Bytes.toString(p.v2().getRow()), p.v1());
			return b;
		}).list();
		int batchSize = l.size();
		if (batchSize == 0) return;
		if (batchSize == 1) {
			Pair<Message, Row> p = l.get(0);
			try {
				Table t = hconn.table(p.v1().table());
				Row req = p.v2();
				if (req instanceof Put) t.put((Put) req);
				else if (req instanceof Delete) t.delete((Delete) req);
				else if (req instanceof Increment) t.increment((Increment) req);
				else if (req instanceof Append) t.append((Append) req);
				succeeded(1);
			} catch (IOException e) {
				failed(Sdream.of1(p.v1()));
			} finally {
				ops.decrementAndGet();
			}
		} else enq(table, l, ops);
	}

	private void enq(String table, List<Pair<Message, Row>> l, AtomicInteger ops) {
		List<Message> vs = Sdream.of(l).map(v -> v.v1()).list();
		List<? extends Row> puts = Sdream.of(l).map(v -> v.v2()).list();
		Object[] results = new Object[l.size()];
		try {
			hconn.table(table).batchCallback(puts, results, (region, row, result) -> ops.decrementAndGet());
			/**
			 * original exception handling in callback
			 * 
			 * <pre>
			 * if (result instanceof Result) succeeded(1);
			 * else {
			 * 	Message m = map.get(Bytes.toString(row));
			 * 	logger().debug(() -> "Hbase failed on: " + m.toString(), result instanceof Throwable ? (Throwable) result
			 * 			: new RuntimeException("Unknown hbase return [" + result.getClass() + "]: " + result.toString()));
			 * 	failed(Sdream.of1(m));
			 * }
			 * </pre>
			 */
		} catch (Exception ex) {
			logger().warn(name() + " write failed [" + Exceptions.unwrap(ex).getMessage() + "], [" + l.size() + "] into fails.");
			List<Message> fails = new CopyOnWriteArrayList<>();
			for (int i = 0; i < results.length; i++)
				if (results[i] instanceof Result) succeeded(1);
				else fails.add(vs.get(i));
			failed(Sdream.of(fails));
		}
	}

	private List<Message> incs(String table, Sdream<Message> values) {
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
