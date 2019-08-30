package net.butfly.albatis.hbase;

import static net.butfly.albacore.utils.Exceptions.unwrap;
import static net.butfly.albacore.utils.collection.Colls.list;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.Connection;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.hbase.utils.Hbases;
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
	public String defaultNamespace;

	private transient HbaseConnection conn;
	private final URISpec target;

	public HbaseOutput(String name, HbaseConnection hconn) throws IOException {
		super(name);
		this.target = hconn.uri();
		this.conn = hconn;
	}

	public HbaseOutput(String name, String defaultNamespace, HbaseConnection hconn) throws IOException {
		super(name);
		this.target = hconn.uri();
		this.conn = hconn;
		this.defaultNamespace = defaultNamespace;
	}

	@Override
	public URISpec target() {
		return target;
	}

	@Override
	public void close() {
		super.close();
		try {
			conn.close();
		} catch (IOException e) {}
	}

	@Override
	public Connection connect() throws IOException {
		if (null == conn) conn = Connection.DriverManager.connect(target);
		return conn;
	}

	@Override
	public Statistic statistic() {
		return new Statistic(this).sizing(Mutation::heapSize).detailing(() -> "Pengding ops: " + opsPending.get())//
				.<Mutation> infoing(r -> Bytes.toString(r.getRow()));
	}

	@Override
	protected void enqsafe(Sdream<Rmap> msgs) {
		Map<Qualifier, List<Rmap>> map = Maps.of();
		msgs.eachs(m -> map.computeIfAbsent(m.table(), t -> Colls.list()).add(m));
		map.forEach((t, l) -> {
			if (l.isEmpty()) return;
			if (1 == l.size()) {
				if (conn.OP_SINGLE) enq1(t.name, op(l.get(0)), l.get(0));
				else enqs(t.name, Colls.list(op(l.get(0))), l);
			} else enq(t, l);
		});
	}

	protected void enq(Qualifier table, List<Rmap> msgs) {
		List<Rmap> origins = Colls.list();
		List<Mutation> puts = Colls.list();

		incs(table, msgs, origins, puts);
		if (1 == puts.size()) {
			if (conn.OP_SINGLE) enq1(table.name, puts.get(0), origins.get(0));
			enqs(table.name, puts, origins);
		} else enqs(table.name, puts, origins);
	}

	protected void enq1(String table, Mutation op, Rmap origin) {
		try {
			conn.put(table, op);
			succeeded(1);
		} catch (IOException e) {
			if (null != origin) failed(Sdream.of1(origin));
		} catch (Throwable e) {
			logger().error("Unknown hbase error for: " + String.valueOf(origin), e);
		}
	}

	protected void enqs(String table, List<Mutation> ops, List<Rmap> origins) {
		List<Rmap> failed = Colls.list();
		AtomicInteger succs = new AtomicInteger();
		try {
			Result[] rs = conn.put(null == defaultNamespace || defaultNamespace.isEmpty() ? table : defaultNamespace + ":" + table, ops.toArray(new Mutation[0]));
			for (int i = 0; i < rs.length; i++) {
				if (null == rs[i]) failed.add(origins.get(i));// error
				else succs.incrementAndGet();
			}
		} catch (IOException e) {
			logger().debug(() -> name() + " write failed [" //
					+ unwrap(e).getMessage().replaceAll("\n\\s+at .*\\)\n", "") // shink stacktrace in error message;
					+ "], [" + ops.size() + "] into fails.");
			failed(Sdream.of(origins));
		} catch (Throwable e) {
			logger().error("Unknown hbase error for: " + origins.toString(), e);
		} finally {
			if (!failed.isEmpty()) failed(Sdream.of(failed));
			int s = succs.get();
			if (s > 0) succeeded(s);
		}
	}

	protected void incs(Qualifier table, List<Rmap> msgs, List<Rmap> origins, List<Mutation> puts) {
		Map<Object, List<Rmap>> incByKeys = Maps.of();
		for (Rmap m : msgs) switch (m.op()) {
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
			for (Rmap m : e.getValue()) for (Map.Entry<String, Object> f : m.entrySet()) merge.compute(f.getKey(), (fn, v) -> lvalue(v)
					+ lvalue(f.getValue()));
			for (String k : merge.keySet()) if (((Long) merge.get(k)).longValue() <= 0) merge.remove(k);
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
