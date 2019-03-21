package net.butfly.albatis.hbase;

import static net.butfly.albacore.utils.collection.Colls.list;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.Connection;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.Rmap.Op;

/**
 * Subject (embedded document serialized as BSON with prefixed column name) writer to hbase.
 */
public class HbaseOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = 3301721159039265076L;
	public static final @HbaseProps String MAX_CONCURRENT_OP_PROP_NAME = HbaseProps.OUTPUT_CONCURRENT_OPS;
	public static final int MAX_CONCURRENT_OP_DEFAULT = Integer.MAX_VALUE;
	public static final int SUGGEST_BATCH_SIZE = 200;
	private transient HbaseConnection conn;
	private final URISpec target;

	public HbaseOutput(String name, HbaseConnection hconn) throws IOException {
		super(name);
		this.target = hconn.uri();
		this.conn = hconn;
	}

	@Override
	public URISpec target() {
		return target;
	}

	@Override
	public Connection connect() throws IOException {
		if (null == conn) conn = Connection.DriverManager.connect(target);
		return conn;
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).sizing(Mutation::heapSize).detailing(() -> "Pengding ops: " + opsPending.get())//
				.<Mutation> sampling(r -> Bytes.toString(r.getRow()));
	}

	@Override
	protected void enqsafe(Sdream<Rmap> msgs) {
		Map<Qualifier, List<Rmap>> map = Maps.of();
		msgs.eachs(m -> map.computeIfAbsent(m.table(), t -> Colls.list()).add(m));
		map.forEach((t, l) -> {
			if (l.isEmpty()) return;
			if (1 == l.size()) enq1(t, op(l.get(0)), l.get(0));
			else enq(t, l);
		});
	}

	protected void enq1(Qualifier table, Mutation op, Rmap origin) {
		s().statsOut(op, o -> {
			try {
				conn.put(table.name, op);
				succeeded(1);
			} catch (IOException e) {
				failed(Sdream.of1(origin));
			}
		});
	}

	protected void enq(Qualifier table, List<Rmap> msgs) {
		List<Rmap> origins = Colls.list();
		List<Mutation> puts = Colls.list();
		incs(table, msgs, origins, puts);

		if (1 == puts.size()) enq1(origins.get(0).table(), puts.get(0), origins.get(0));
		else enqs(table.name, origins, puts);
	}

	protected void enqs(String table, List<Rmap> origins, List<Mutation> enqs) {
		Object[] results = new Object[enqs.size()];
		try {
			s().statsOuts(enqs, c -> {
				try {
					conn.table(table).batch(enqs, results);
				} catch (Exception e) {
					String err = Exceptions.unwrap(e).getMessage();
					err = err.replaceAll("\n\\s+at .*\\)\n", "");
					// shink stacktrace in error message
					logger().debug(name() + " write failed [" + err + "], [" + enqs.size() + "] into fails.");
					failed(Sdream.of(origins));
				}
			});
		} finally {
			List<Rmap> failed = Colls.list();
			int succs = 0;
			for (int i = 0; i < results.length; i++) {
				if (null == results[i]) // error
					failed.add(origins.get(i));
				else if (results[i] instanceof Result) succs++;
				else logger().error("HbaseOutput unknown: [" + results[i].toString() + "], pending: " + opsPending.get() + "]");
			}
			if (!failed.isEmpty()) failed(Sdream.of(failed));
			if (succs > 0) succeeded(succs);
		}
	}

	protected void incs(Qualifier table, List<Rmap> msgs, List<Rmap> origins, List<Mutation> puts) {
		Map<Object, List<Rmap>> incByKeys = Maps.of();
		for (Rmap m : msgs)
			switch (m.op()) {
			case Op.INCREASE:
				incByKeys.compute(m.key(), (k, l) -> {
					if (null == l) l = list();
					l.add(m);
					return l;
				});
				break;
			case Op.DELETE:
				logger().error("Message marked as delete but ignore: " + m.toString());
				break;
			default:
				Mutation r = op(m);
				if (null != r) {
					origins.add(m);
					puts.add(r);
				}
			}

		for (Entry<Object, List<Rmap>> e : incByKeys.entrySet()) {
			Rmap merge = new Rmap(table, e.getKey()).op(Op.INCREASE);
			for (Rmap m : e.getValue())
				for (Map.Entry<String, Object> f : m.entrySet())
					merge.compute(f.getKey(), (fn, v) -> lvalue(v) + lvalue(f.getValue()));
			for (String k : merge.keySet())
				if (((Long) merge.get(k)).longValue() <= 0) merge.remove(k);
			if (!merge.isEmpty()) {
				Mutation r = op(merge);
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

	protected Mutation op(Rmap m) {
		return Hbases.Results.op(m);
	}
}
