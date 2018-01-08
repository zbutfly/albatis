package net.butfly.albatis.hbase;

import static net.butfly.albacore.utils.collection.Colls.list;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Message.Op;
import net.butfly.albatis.io.SafeKeyOutput;

public final class HbaseOutput extends SafeKeyOutput<String, Message> {
	public static final @HbaseProps String MAX_CONCURRENT_OP_PROP_NAME = HbaseProps.OUTPUT_CONCURRENT_OPS;
	public static final int MAX_CONCURRENT_OP_DEFAULT = Integer.MAX_VALUE;
	public static final int SUGGEST_BATCH_SIZE = 200;
	private final HbaseConnection hconn;

	public HbaseOutput(String name, HbaseConnection hconn) throws IOException {
		super(name);
		this.hconn = hconn;
		open();
	}

	@Override
	public Statistic<Mutation> trace() {
		return new Statistic<Mutation>(Mutation.class)//
				.sizing(Mutation::heapSize).detailing(() -> "Pengding ops: " + opsPending.get());
	}

	@Override
	protected void enqSafe(String table, Sdream<Message> msgs) {
		List<Message> origins = Colls.list();
		List<Mutation> puts = Colls.list();
		incs(table, msgs.filter(m -> null != m.key()), origins, puts);

		switch (puts.size()) {
		case 0:
			opsPending.decrementAndGet();
			return;
		case 1:
			try {
				Table t = hconn.table(origins.get(0).table());
				Mutation req = puts.get(0);
				if (req instanceof Put) t.put((Put) req);
				else if (req instanceof Delete) t.delete((Delete) req);
				else if (req instanceof Increment) t.increment((Increment) req);
				else if (req instanceof Append) t.append((Append) req);
				succeeded(1);
				stats(req);
			} catch (IOException e) {
				failed(Sdream.of(origins));
			} finally {
				opsPending.decrementAndGet();
			}
			return;
		default:
			enqAsync(table, origins, puts);
		}
	}

	protected void enqAsync(String table, List<Message> origins, List<Mutation> enqs) {
		Object[] results = new Object[enqs.size()];
		try {
			hconn.table(table).batch(enqs, results);
		} catch (Exception ex) {
			logger().error(name() + " write failed [" + Exceptions.unwrap(ex).getMessage() + "], [" + enqs.size() + "] into fails.");
			failed(Sdream.of(origins));
		} finally {
			List<Message> failed = Colls.list();
			int succs = 0;
			for (int i = 0; i < results.length; i++) {
				if (null == results[i]) // error
					failed.add(origins.get(i));
				else if (results[i] instanceof Result) succs++;
				else logger().error("HbaseOutput unknown: [" + results[i].toString() + "], pending: " + opsPending.get() + "]");
			}
			if (!failed.isEmpty()) failed(Sdream.of(failed));
			if (succs > 0) succeeded(succs);
			opsPending.decrementAndGet();
			stats(enqs);
			// logger().error("INFO: HbaseOutput batch [messages: " + origins.size() + ", actions: " + enqs.size() + "], failed " + failed
			// .size() + ", success: " + succs + ", pending: " + opsPending.get() + ".");
		}
	}

	private void incs(String table, Sdream<Message> values, List<Message> origins, List<Mutation> puts) {
		Map<String, List<Message>> incByKeys = Maps.of();
		values.eachs(m -> {
			if (m.op() == Op.INCREASE) {
				incByKeys.compute(m.key(), (k, l) -> {
					if (null == l) l = list();
					l.add(m);
					return l;
				});
			} else {
				Mutation r = Hbases.Results.put(m);
				if (null != r) {
					origins.add(m);
					puts.add(r);
				}
			}
		});
		for (Map.Entry<String, List<Message>> e : incByKeys.entrySet()) {
			Message merge = new Message(table, e.getKey()).op(Op.INCREASE);
			for (Message m : e.getValue())
				for (Map.Entry<String, Object> f : m.entrySet())
					merge.compute(f.getKey(), (fn, v) -> lvalue(v) + lvalue(f.getValue()));
			for (String k : merge.keySet())
				if (((Long) merge.get(k)).longValue() <= 0) merge.remove(k);
			if (!merge.isEmpty()) {
				Mutation r = Hbases.Results.put(merge);
				if (null != r) {
					origins.add(merge);
					puts.add(r);
				}
			}
		}
	}

	private long lvalue(Object o) {
		return null != o && Number.class.isAssignableFrom(o.getClass()) ? ((Number) o).longValue() : 0;
	}

	@Override
	public String partition(Message v) {
		return v.table();
	}
}
